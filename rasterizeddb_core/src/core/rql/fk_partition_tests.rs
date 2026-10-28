//! Foreign keys and partitioned tables below the wire (`docs/partitioning.md`
//! §10, phase 3 P4): what a check reads, and the reverse index.
//!
//! - A check of a partitioned parent routes: each probe reads the one
//!   partition its key hashes to, never every partition, and a key that
//!   cannot route is refused rather than read everywhere or skipped.
//! - `Database.fk_reverse` agrees with every schema after each DDL that
//!   changes a key (CREATE, DROP, RENAME, ADD and DROP CONSTRAINT, DROP
//!   COLUMN, DROP … CASCADE, the partitioned forms) and after a reopen.

use crate::core::database::Database;
use crate::core::rql::executor::execute;
use crate::core::rql::lexer_s1::recognize_query_purpose;
use rclite::Arc;

fn init_globals() {
    let _ = crate::MAX_PERMITS_THREADS.set(4);
    let _ = crate::BATCH_SIZE.set(1024);
}

async fn open(dir: &std::path::Path) -> Arc<Database> {
    Arc::new(Database::new_named(&dir.to_string_lossy(), false, "fkpartitiontests").await)
}

async fn run(database: &Arc<Database>, sql: &str) -> Result<Vec<u8>, String> {
    let purpose = recognize_query_purpose(sql).unwrap_or_else(|| panic!("unrecognised: {sql}"));
    let response = execute(purpose, database.clone()).await;
    if response.first() == Some(&3) {
        Err(String::from_utf8_lossy(&response[1..]).to_string())
    } else {
        Ok(response)
    }
}

#[track_caller]
fn ok(result: Result<Vec<u8>, String>, sql: &str) -> Vec<u8> {
    result.unwrap_or_else(|e| panic!("`{sql}` failed: {e}"))
}

async fn run_ok(database: &Arc<Database>, sql: &str) -> Vec<u8> {
    ok(run(database, sql).await, sql)
}

fn rows_of(response: &[u8]) -> usize {
    crate::core::row::row::vec_into_rows(&response[1..]).expect("rows").len()
}

/// The partitions a statement read on `table`, by pid.
async fn reads_of(database: &Arc<Database>, table: &str, sql: &str) -> (Result<Vec<u8>, String>, Vec<u32>) {
    let uid = database.partitioned_table(table).unwrap().uid();
    crate::core::rql::row_source::test_hooks::take_reads(uid);
    let out = run(database, sql).await;
    (out, crate::core::rql::row_source::test_hooks::take_reads(uid))
}

fn distinct(pids: &[u32]) -> Vec<u32> {
    let mut d = pids.to_vec();
    d.sort();
    d.dedup();
    d
}

/// Every check of a partitioned parent reads only the partitions its keys
/// hash to: an insert's probe and its recheck under the key lock read the
/// key's one partition; a multi-row insert's batched probe at most one per
/// key; a missing key at most one; a parent DELETE's key read routes on its
/// `WHERE`. And each was a partitioned probe of the parent side, so a check
/// that skipped the parent would fail here too.
#[tokio::test(flavor = "multi_thread")]
async fn a_check_of_a_partitioned_parent_reads_one_partition_per_key() {
    init_globals();
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path()).await;
    run_ok(&db, "CREATE TABLE rt_p (id BIGINT NOT NULL PRIMARY KEY, label TEXT) PARTITION BY HASH (id) PARTITIONS 16").await;
    let values: Vec<String> = (1..=96).map(|i| format!("({i}, 'p{i}')")).collect();
    run_ok(&db, &format!("INSERT INTO rt_p (id, label) VALUES {}", values.join(", "))).await;
    run_ok(&db, "CREATE TABLE rt_c (cid BIGINT NOT NULL PRIMARY KEY, pid BIGINT REFERENCES rt_p (id) ON DELETE CASCADE)").await;
    let materialized = {
        let (_, all) = reads_of(&db, "rt_p", "SELECT count(*) FROM rt_p").await;
        distinct(&all)
    };
    assert!(materialized.len() > 8, "the parent spans most partitions: {materialized:?}");

    let (_, home) = reads_of(&db, "rt_p", "SELECT id FROM rt_p WHERE id = 5").await;
    assert_eq!(home.len(), 1, "a key's own partition: {home:?}");
    crate::core::rql::executor::fk_probe_hooks::take();

    let (out, reads) = reads_of(&db, "rt_p", "INSERT INTO rt_c (cid, pid) VALUES (1, 5)").await;
    ok(out, "insert");
    assert_eq!(reads, vec![home[0], home[0]], "the probe and its recheck, each in the key's partition");
    let probes = crate::core::rql::executor::fk_probe_hooks::take();
    assert!(
        probes.iter().filter(|(t, parent)| t == "rt_p" && *parent).count() >= 2,
        "the parent was probed as a partitioned parent: {probes:?}"
    );

    let (out, reads) = reads_of(&db, "rt_p", "INSERT INTO rt_c (cid, pid) VALUES (2, 5000)").await;
    assert!(out.unwrap_err().contains("violates foreign key constraint"));
    assert!(distinct(&reads).len() <= 1, "a missing key reads at most its own partition: {reads:?}");

    let tuples: Vec<String> = (10..20).map(|i| format!("({i}, {})", i * 3)).collect();
    let (out, reads) = reads_of(&db, "rt_p", &format!("INSERT INTO rt_c (cid, pid) VALUES {}", tuples.join(", "))).await;
    ok(out, "multi-row insert");
    assert!(distinct(&reads).len() <= 10, "ten keys read at most ten partitions: {reads:?}");
    assert!(distinct(&reads).len() < materialized.len(), "never every partition: {reads:?}");

    // The parent's DELETE: its keys read on its own route, then the child's
    // action, then the write, all in the key's partition.
    let (out, reads) = reads_of(&db, "rt_p", "DELETE FROM rt_p WHERE id = 5").await;
    ok(out, "delete");
    assert_eq!(distinct(&reads), home, "the delete's key read routes on its WHERE: {reads:?}");
    assert_eq!(rows_of(&run_ok(&db, "SELECT cid FROM rt_c WHERE pid = 5").await), 0, "cascaded");
    assert_eq!(rows_of(&run_ok(&db, "SELECT id FROM rt_p WHERE id = 5").await), 0, "deleted");
}

/// A key that names a partitioned parent by a column other than its whole
/// partition key cannot be declared (42830), so one can only come from a
/// record written behind the DDL's back. Its checks are refused: never read
/// in every partition, and never skipped (the inserted value exists in some
/// partition, so a skipped or fanned-out check would let the row in).
#[tokio::test(flavor = "multi_thread")]
async fn a_check_that_cannot_route_is_refused_never_skipped() {
    init_globals();
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path()).await;
    run_ok(&db, "CREATE TABLE nr_p (id BIGINT NOT NULL PRIMARY KEY, alt BIGINT) PARTITION BY HASH (id) PARTITIONS 8").await;
    run_ok(&db, "INSERT INTO nr_p (id, alt) VALUES (1, 10), (2, 20), (3, 30)").await;
    run_ok(&db, "CREATE TABLE nr_c (cid BIGINT NOT NULL PRIMARY KEY, x BIGINT)").await;
    run_ok(&db, "INSERT INTO nr_c (cid, x) VALUES (1, 20)").await;
    let e = run(&db, "ALTER TABLE nr_c ADD FOREIGN KEY (x) REFERENCES nr_p (alt)").await.unwrap_err();
    assert!(e.contains("there is no unique constraint matching given keys"), "{e}");
    // Written below the DDL, as an older or a broken path might have.
    let io = db.table_for_tests("nr_c").unwrap().io_schema.clone();
    crate::core::rql::foreign_key::append(
        io,
        &crate::core::rql::foreign_key::ForeignKey {
            name: "behind".to_string(),
            column: "x".to_string(),
            parent_table: "nr_p".to_string(),
            parent_column: "alt".to_string(),
            on_delete: crate::core::rql::foreign_key::ReferentialAction::NoAction,
            on_update: crate::core::rql::foreign_key::ReferentialAction::NoAction,
            is_deleted: false,
        },
    )
    .await
    .unwrap();
    db.reload_table("nr_c").await.unwrap();
    let e = run(&db, "INSERT INTO nr_c (cid, x) VALUES (2, 30)").await.unwrap_err();
    assert!(e.contains("cannot be checked") && e.contains("not supported"), "{e}");
    assert_eq!(rows_of(&run_ok(&db, "SELECT cid FROM nr_c").await), 1, "nothing written");
    // The parent side reads the child, which needs no route: the row still
    // referencing `alt` = 20 refuses the parent's DELETE.
    let e = run(&db, "DELETE FROM nr_p WHERE id = 2").await.unwrap_err();
    assert!(e.contains("violates foreign key constraint"), "{e}");
    assert_eq!(rows_of(&run_ok(&db, "SELECT id FROM nr_p").await), 3, "nothing deleted");
    run_ok(&db, "DELETE FROM nr_p WHERE id = 3").await;
}

/// `fk_reverse` against a walk of every schema after each DDL that changes
/// what references what, on plain and partitioned tables, and after a
/// reopen (rebuilt from the schemas at open).
#[tokio::test(flavor = "multi_thread")]
async fn the_reverse_index_agrees_with_the_schemas_after_every_ddl() {
    init_globals();
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path()).await;
    let steps = [
        "CREATE TABLE ri_a (id BIGINT NOT NULL PRIMARY KEY) PARTITION BY HASH (id) PARTITIONS 4",
        "CREATE TABLE ri_b (id BIGINT NOT NULL PRIMARY KEY, a BIGINT REFERENCES ri_a (id))",
        "CREATE TABLE ri_c (id BIGINT NOT NULL PRIMARY KEY, a BIGINT, b BIGINT, \
         FOREIGN KEY (b) REFERENCES ri_b (id)) PARTITION BY HASH (id) PARTITIONS 4",
        "ALTER TABLE ri_c ADD FOREIGN KEY (a) REFERENCES ri_a (id)",
        "ALTER TABLE ri_c ADD CONSTRAINT second FOREIGN KEY (a) REFERENCES ri_a (id) ON DELETE CASCADE",
        "ALTER TABLE ri_a RENAME TO ri_a2",
        "ALTER TABLE ri_c RENAME TO ri_c2",
        "ALTER TABLE ri_c2 DROP CONSTRAINT second",
        "ALTER TABLE ri_c2 RENAME COLUMN b TO bb",
        "ALTER TABLE ri_b RENAME COLUMN id TO bid",
        "ALTER TABLE ri_c2 DROP COLUMN a",
        "CREATE TABLE ri_self (id BIGINT NOT NULL PRIMARY KEY, up BIGINT REFERENCES ri_self (id)) \
         PARTITION BY HASH (id) PARTITIONS 4",
        "DROP TABLE ri_b CASCADE",
        "DROP TABLE ri_self",
    ];
    for sql in steps {
        run_ok(&db, sql).await;
        db.assert_fk_reverse_complete();
    }
    for parent in ["ri_a", "ri_a2", "ri_b", "ri_c", "ri_c2", "ri_self", "nothing"] {
        let _ = db.referencing_keys(parent);
    }
    assert_eq!(db.referencing_keys("ri_a").len(), 0, "renamed away");
    // ri_b's key followed the rename and went with ri_b; ri_c's went with its
    // column and by name.
    assert_eq!(db.referencing_keys("ri_a2").len(), 0);
    assert_eq!(db.referencing_keys("ri_b").len(), 0, "dropped with CASCADE");
    run_ok(&db, "ALTER TABLE ri_c2 ADD FOREIGN KEY (bb) REFERENCES ri_a2 (id)").await;
    // A plain child too: plain tables enter the map before the database
    // exists at open, so only the rebuild files their keys.
    run_ok(&db, "CREATE TABLE ri_d (id BIGINT NOT NULL PRIMARY KEY, a BIGINT REFERENCES ri_a2 (id))").await;
    db.assert_fk_reverse_complete();
    drop(db);
    crate::core::wal::remove_for(&dir.path().to_string_lossy());
    let db = open(dir.path()).await;
    db.assert_fk_reverse_complete();
    let children: Vec<String> = db.referencing_keys("ri_a2").into_iter().map(|(c, _)| c).collect();
    assert_eq!(children, vec!["ri_c2".to_string(), "ri_d".to_string()]);
}

/// `ADD FOREIGN KEY` validates every row, in every partition, and names the
/// value that has no parent wherever it falls among the child's values (the
/// chunks it probes are split until it is alone), and stores nothing.
#[tokio::test(flavor = "multi_thread")]
async fn add_names_the_one_value_without_a_parent() {
    init_globals();
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path()).await;
    run_ok(&db, "CREATE TABLE vb_p (id BIGINT NOT NULL PRIMARY KEY) PARTITION BY HASH (id) PARTITIONS 8").await;
    let parents: Vec<String> = (1..=1200).filter(|i| *i != 777).map(|i| format!("({i})")).collect();
    run_ok(&db, &format!("INSERT INTO vb_p (id) VALUES {}", parents.join(", "))).await;
    run_ok(&db, "CREATE TABLE vb_c (cid BIGINT NOT NULL PRIMARY KEY, pid BIGINT) PARTITION BY HASH (cid) PARTITIONS 8").await;
    let children: Vec<String> = (1..=1200).map(|i| format!("({i}, {i})")).collect();
    run_ok(&db, &format!("INSERT INTO vb_c (cid, pid) VALUES {}", children.join(", "))).await;
    let e = run(&db, "ALTER TABLE vb_c ADD FOREIGN KEY (pid) REFERENCES vb_p (id)").await.unwrap_err();
    assert!(e.contains("key (pid)=(777) is not present in table \"vb_p\""), "{e}");
    assert!(db.partitioned_table("vb_c").unwrap().shell().schema.foreign_keys.is_empty(), "nothing stored");
    run_ok(&db, "INSERT INTO vb_p (id) VALUES (777)").await;
    run_ok(&db, "ALTER TABLE vb_c ADD FOREIGN KEY (pid) REFERENCES vb_p (id)").await;
    assert_eq!(db.partitioned_table("vb_c").unwrap().shell().schema.foreign_keys.len(), 1);
}

/// A column change of a parent rewrites its children's FK records with its
/// own schema files, one intent over all of them; a partitioned child's are
/// its shell's `__pt_<uid>_schema*` files, so they are held aside under that
/// stem. A failure after the work puts every one back: after a reopen the
/// child's key still names the column the parent still has.
#[tokio::test(flavor = "multi_thread")]
async fn a_failed_parent_column_rename_puts_a_partitioned_childs_key_back() {
    init_globals();
    let dir = tempfile::tempdir().unwrap();
    {
        let db = open(dir.path()).await;
        run_ok(&db, "CREATE TABLE rn_p (id BIGINT NOT NULL PRIMARY KEY)").await;
        run_ok(&db, "INSERT INTO rn_p (id) VALUES (1), (2)").await;
        run_ok(&db, "CREATE TABLE rn_c (cid BIGINT NOT NULL PRIMARY KEY, pid BIGINT REFERENCES rn_p (id)) \
                     PARTITION BY HASH (cid) PARTITIONS 4").await;
        run_ok(&db, "INSERT INTO rn_c (cid, pid) VALUES (1, 1), (2, 2)").await;
        let guard = crate::core::power_loss::failpoint::arm(
            "test.rewrite.metadata_after_work",
            dir.path(),
            1,
            std::sync::Arc::new(|_hit: &crate::core::power_loss::failpoint::Hit<'_>| {
                crate::core::power_loss::failpoint::Outcome::Fail(std::io::Error::other("injected failure"))
            }),
        );
        let out = run(&db, "ALTER TABLE rn_p RENAME COLUMN id TO ident").await;
        assert_eq!(guard.fired(), 1, "{out:?}");
        drop(guard);
        assert!(out.is_err(), "reported success over an injected failure");
        drop(db);
    }
    crate::core::wal::remove_for(&dir.path().to_string_lossy());
    let db = open(dir.path()).await;
    let fks = db.partitioned_table("rn_c").unwrap().shell().schema.foreign_keys.clone();
    assert_eq!(fks.iter().map(|f| f.parent_column.as_str()).collect::<Vec<_>>(), vec!["id"], "{fks:?}");
    let e = run(&db, "INSERT INTO rn_c (cid, pid) VALUES (3, 9)").await.unwrap_err();
    assert!(e.contains("violates foreign key constraint"), "{e}");
    run_ok(&db, "INSERT INTO rn_c (cid, pid) VALUES (3, 2)").await;
}

/// A key the router reads no point in: `k = -7` (a signed number is no key
/// point to `route_where`, which then routes everywhere), and a text key
/// holding a quote. The check routes each value by its key bytes instead,
/// as an INSERT of it stores them (`lexer_ir::encode_column_value`): one
/// partition per key, the key found, a missing one refused, never read in
/// every partition. The twin found the
/// negative case at 0x5eedf1a1: such a parent's children were refused.
#[tokio::test(flavor = "multi_thread")]
async fn a_key_the_router_cannot_read_still_routes_by_its_bytes() {
    init_globals();
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path()).await;
    run_ok(&db, "CREATE TABLE ng_p (id BIGINT NOT NULL PRIMARY KEY) PARTITION BY HASH (id) PARTITIONS 16").await;
    let parents: Vec<String> = (-40..=40).filter(|i| *i != 0).map(|i| format!("({i})")).collect();
    run_ok(&db, &format!("INSERT INTO ng_p (id) VALUES {}", parents.join(", "))).await;
    run_ok(&db, "CREATE TABLE ng_c (cid BIGINT NOT NULL PRIMARY KEY, pid BIGINT REFERENCES ng_p (id))").await;
    let materialized = distinct(&reads_of(&db, "ng_p", "SELECT count(*) FROM ng_p").await.1);
    let (out, reads) = reads_of(&db, "ng_p", "INSERT INTO ng_c (cid, pid) VALUES (1, -7)").await;
    ok(out, "a negative parent key");
    assert_eq!(distinct(&reads).len(), 1, "the key's one partition: {reads:?}");
    let (out, reads) = reads_of(&db, "ng_p", "INSERT INTO ng_c (cid, pid) VALUES (2, -77)").await;
    assert!(out.unwrap_err().contains("violates foreign key constraint"));
    assert!(distinct(&reads).len() <= 1, "{reads:?}");
    let tuples: Vec<String> = (10..20).map(|i| format!("({i}, -{})", i * 2)).collect();
    let (out, reads) = reads_of(&db, "ng_p", &format!("INSERT INTO ng_c (cid, pid) VALUES {}", tuples.join(", "))).await;
    ok(out, "negative keys in a batch");
    assert!(distinct(&reads).len() <= 10 && distinct(&reads).len() < materialized.len(), "{reads:?}");
    // Cast, as an ORM renders `$n::bigint`, and through CAST: each still
    // its one partition (P4 round 2: these were refused with 0A000).
    for (cid, value) in [(30, "-8::bigint"), (31, "CAST(-9 AS BIGINT)"), (32, "-11::int"), (33, "'-12'")] {
        let (out, reads) = reads_of(&db, "ng_p", &format!("INSERT INTO ng_c (cid, pid) VALUES ({cid}, {value})")).await;
        ok(out, value);
        assert_eq!(distinct(&reads).len(), 1, "{value}: {reads:?}");
    }
    let e = run(&db, "INSERT INTO ng_c (cid, pid) VALUES (34, -88::bigint)").await.unwrap_err();
    assert!(e.contains("violates foreign key constraint"), "{e}");
    let e = run(&db, "DELETE FROM ng_p WHERE id = -7").await.unwrap_err();
    assert!(e.contains("violates foreign key constraint"), "{e}");

    run_ok(&db, "CREATE TABLE qt_p (code TEXT NOT NULL PRIMARY KEY) PARTITION BY HASH (code) PARTITIONS 8").await;
    run_ok(&db, "INSERT INTO qt_p (code) VALUES ('a''b'), ('plain'), ('x y')").await;
    run_ok(&db, "CREATE TABLE qt_c (cid BIGINT NOT NULL PRIMARY KEY, code TEXT REFERENCES qt_p (code))").await;
    for (i, code) in ["'a''b'", "'plain'", "'x y'"].iter().enumerate() {
        let (out, reads) = reads_of(&db, "qt_p", &format!("INSERT INTO qt_c (cid, code) VALUES ({i}, {code})")).await;
        ok(out, code);
        assert_eq!(distinct(&reads).len(), 1, "{code}: {reads:?}");
    }
    let e = run(&db, "INSERT INTO qt_c (cid, code) VALUES (9, 'a''c')").await.unwrap_err();
    assert!(e.contains("violates foreign key constraint"), "{e}");
    run_ok(&db, "ALTER TABLE qt_c DROP CONSTRAINT qt_c_code_fkey").await;
    run_ok(&db, "ALTER TABLE qt_c ADD FOREIGN KEY (code) REFERENCES qt_p (code)").await;
}

/// Peak memory of `ADD FOREIGN KEY`'s validation over a 200,000-row child
/// with 200,000 distinct values (P4 round 2). Measured, not asserted: the
/// peak (`VmHWM`) is reset just before the ADD (`/proc/self/clear_refs` 5),
/// so it is the ADD's own, over the resident set it started from.
///
/// `cargo test -p rasterizeddb_core --lib -- --ignored validation_peak_memory --nocapture`
#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn validation_peak_memory() {
    fn status(field: &str) -> u64 {
        let text = std::fs::read_to_string("/proc/self/status").unwrap();
        let line = text.lines().find(|l| l.starts_with(field)).unwrap();
        line.split_whitespace().nth(1).unwrap().parse().unwrap()
    }
    init_globals();
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path()).await;
    const ROWS: usize = 200_000;
    run_ok(&db, "CREATE TABLE pm_p (id BIGINT NOT NULL PRIMARY KEY)").await;
    run_ok(&db, "CREATE TABLE pm_c (cid BIGINT NOT NULL PRIMARY KEY, pid BIGINT)").await;
    for start in (0..ROWS).step_by(2000) {
        let ids: Vec<String> = (start..start + 2000).map(|i| format!("({i})")).collect();
        run_ok(&db, &format!("INSERT INTO pm_p (id) VALUES {}", ids.join(", "))).await;
        let rows: Vec<String> = (start..start + 2000).map(|i| format!("({i}, {i})")).collect();
        run_ok(&db, &format!("INSERT INTO pm_c (cid, pid) VALUES {}", rows.join(", "))).await;
    }
    std::fs::write("/proc/self/clear_refs", "5").unwrap();
    let rss = status("VmRSS:");
    let started = std::time::Instant::now();
    run_ok(&db, "ALTER TABLE pm_c ADD FOREIGN KEY (pid) REFERENCES pm_p (id)").await;
    let peak = status("VmHWM:");
    println!(
        "validation of {ROWS} rows: {:?}, VmRSS before {rss} kB, VmHWM during {peak} kB, peak over start {} kB",
        started.elapsed(),
        peak.saturating_sub(rss)
    );
}

/// One snapshot for a partitioned DELETE's FK work and its write (P4 round
/// 2). A fan-out `DELETE … WHERE label = 'x'` of a referenced parent reads
/// the keys it will remove partition by partition under the statement's
/// snapshot; between two of those reads another session commits a new
/// matching parent, in a partition read later, and a child of it. The write
/// must use the same snapshot: the new parent, whose children were never
/// checked, survives, and no child is left without a parent. A write taking
/// its own snapshot would delete it and orphan the child.
#[tokio::test(flavor = "multi_thread")]
async fn a_partitioned_delete_writes_the_rows_its_keys_were_read_from() {
    use std::sync::atomic::{AtomicBool, Ordering};
    init_globals();
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path()).await;
    run_ok(&db, "CREATE TABLE sp_p (id BIGINT NOT NULL PRIMARY KEY, label TEXT) PARTITION BY HASH (id) PARTITIONS 8").await;
    let parents: Vec<String> = (1..=40).map(|i| format!("({i}, 'x')")).collect();
    run_ok(&db, &format!("INSERT INTO sp_p (id, label) VALUES {}", parents.join(", "))).await;
    run_ok(&db, "CREATE TABLE sp_c (cid BIGINT NOT NULL PRIMARY KEY, pid BIGINT REFERENCES sp_p (id))").await;
    // A key whose partition is read after the first one the DELETE reads.
    let first = *distinct(&reads_of(&db, "sp_p", "SELECT count(*) FROM sp_p WHERE label = 'x'").await.1).first().unwrap();
    let mut late = None;
    for k in 1000..1200 {
        let (_, pids) = reads_of(&db, "sp_p", &format!("SELECT id FROM sp_p WHERE id = {k}")).await;
        if pids.first().is_some_and(|p| *p > first) {
            late = Some(k);
            break;
        }
    }
    let late = late.expect("a key in a later partition");

    let fired = std::sync::Arc::new(AtomicBool::new(false));
    let hook: crate::core::rql::row_source::test_hooks::Hook = {
        let (db, fired) = (db.clone(), fired.clone());
        std::sync::Arc::new(move || {
            let (db, fired) = (db.clone(), fired.clone());
            Box::pin(async move {
                if fired.swap(true, Ordering::SeqCst) {
                    return;
                }
                run_ok(&db, &format!("INSERT INTO sp_p (id, label) VALUES ({late}, 'x')")).await;
                run_ok(&db, &format!("INSERT INTO sp_c (cid, pid) VALUES (900, {late})")).await;
            })
        })
    };
    let uid = db.partitioned_table("sp_p").unwrap().uid();
    crate::core::rql::row_source::test_hooks::set_after_first(uid, Some(hook));
    let out = run(&db, "DELETE FROM sp_p WHERE label = 'x'").await;
    crate::core::rql::row_source::test_hooks::set_after_first(uid, None);
    assert!(fired.load(Ordering::SeqCst), "the writer never ran between two partition reads");
    ok(out, "delete");
    assert_eq!(rows_of(&run_ok(&db, &format!("SELECT id FROM sp_p WHERE id = {late}")).await), 1, "the late parent was deleted");
    let orphans = run_ok(&db, "SELECT cid FROM sp_c WHERE pid NOT IN (SELECT id FROM sp_p)").await;
    assert_eq!(rows_of(&orphans), 0, "a child was left without its parent");
    assert_eq!(rows_of(&run_ok(&db, "SELECT id FROM sp_p").await), 1);
}

/// A parent DELETE of 10,000 keys whose child (40,000 rows) is partitioned
/// by another column, so every child-side probe reads every partition (P4
/// round 2): the NO ACTION check probes the child for all 10,000 keys and
/// finds none. Timed, not asserted. `cargo test -p rasterizeddb_core --lib
/// -- --ignored child_probe_chunk_timing --nocapture`
#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn child_probe_chunk_timing() {
    init_globals();
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path()).await;
    const KEYS: usize = 10_000;
    run_ok(&db, "CREATE TABLE ct_p (id BIGINT NOT NULL PRIMARY KEY)").await;
    run_ok(&db, "CREATE TABLE ct_c (cid BIGINT NOT NULL PRIMARY KEY, pid BIGINT REFERENCES ct_p (id)) \
                 PARTITION BY HASH (cid) PARTITIONS 8").await;
    for start in (0..KEYS + 1000).step_by(1000) {
        let ids: Vec<String> = (start..start + 1000).map(|i| format!("({i})")).collect();
        run_ok(&db, &format!("INSERT INTO ct_p (id) VALUES {}", ids.join(", "))).await;
    }
    for start in (0..40_000).step_by(2000) {
        let rows: Vec<String> = (start..start + 2000).map(|i| format!("({i}, {})", KEYS + i % 1000)).collect();
        run_ok(&db, &format!("INSERT INTO ct_c (cid, pid) VALUES {}", rows.join(", "))).await;
    }
    let (out, reads) = {
        let started = std::time::Instant::now();
        let (out, reads) = reads_of(&db, "ct_c", &format!("DELETE FROM ct_p WHERE id < {KEYS}")).await;
        println!("DELETE of {KEYS} parents over a fanned-out 40,000-row child: {:?}", started.elapsed());
        (out, reads)
    };
    ok(out, "delete");
    println!("child partition scans: {}", reads.len());
}
