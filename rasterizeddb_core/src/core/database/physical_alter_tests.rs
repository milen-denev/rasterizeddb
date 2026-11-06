//! Physical ALTER of a partitioned table (docs/partitioning.md "Phase 6 P8
//! as built"): a populated `ADD COLUMN`, `ALTER COLUMN … TYPE` and `DROP NOT
//! NULL` rewrite every version into a new uid, committed by the catalogue,
//! and answer exactly as the same statements on a plain twin; a failure or
//! a crash before the commit point leaves the old table, one after it the
//! new, and the losing uid's files are gone by the next open.

use std::path::Path;

use rclite::Arc;

use super::Database;
use crate::core::partition::files::{self, PartFile};
use crate::core::power_loss::failpoint;

async fn open(dir: &Path) -> Arc<Database> {
    let _ = crate::MAX_PERMITS_THREADS.set(4);
    let _ = crate::BATCH_SIZE.set(512);
    let _ = crate::core::processor::concurrent_processor::ENABLE_CACHE.set(false);
    Arc::new(Database::try_new_named(&dir.to_string_lossy(), false, "palter").await.expect("open the database"))
}

fn close(dir: &Path, db: Arc<Database>) {
    drop(db);
    crate::core::wal::remove_for(&dir.to_string_lossy());
}

async fn try_exec(db: &Arc<Database>, sql: &str) -> Result<Vec<u8>, String> {
    let purpose = crate::core::rql::lexer_s1::recognize_query_purpose(sql).ok_or_else(|| format!("parse {sql}"))?;
    let out = crate::core::rql::executor::execute_with_sync(purpose, db.clone(), true).await;
    if out.is_empty() || out[0] == 3 {
        return Err(format!("{sql}: {}", String::from_utf8_lossy(out.get(1..).unwrap_or_default())));
    }
    Ok(out)
}

async fn exec(db: &Arc<Database>, sql: &str) -> Vec<u8> {
    try_exec(db, sql).await.unwrap_or_else(|e| panic!("{e}"))
}

async fn exec_in(db: &Arc<Database>, tx: crate::core::mvcc::transaction::TxId, sql: &str) {
    let purpose = crate::core::rql::lexer_s1::recognize_query_purpose(sql).unwrap_or_else(|| panic!("cannot parse {sql}"));
    let out = Box::pin(crate::core::rql::executor::execute_with_context(purpose, db.clone(), 0, tx)).await;
    assert!(!out.is_empty() && out[0] != 3, "{sql}: {}", String::from_utf8_lossy(&out));
}

/// Both tables' answers to `select` (with `{t}`), which must be equal.
async fn same(db: &Arc<Database>, select: &str, at: &str) -> Vec<u8> {
    let p = exec(db, &select.replace("{t}", "pt")).await;
    let q = exec(db, &select.replace("{t}", "pq")).await;
    assert_eq!(p, q, "{at}: `{select}` differs from the plain twin");
    p
}

async fn compare(db: &Arc<Database>, at: &str) {
    same(db, "SELECT * FROM {t} ORDER BY tenant_id, id", at).await;
    same(db, "SELECT COUNT(*), SUM(id) FROM {t} WHERE tenant_id = 7", at).await;
    same(db, "SELECT tenant_id, COUNT(*) FROM {t} GROUP BY tenant_id ORDER BY tenant_id", at).await;
}

/// Twins: `pt` partitioned by tenant, `pq` plain, the same rows and the
/// same dead and aborted versions written to both.
async fn twins(dir: &Path) -> Arc<Database> {
    let db = open(dir).await;
    for (name, tail) in [("pt", " PARTITION BY HASH (tenant_id) PARTITIONS 6"), ("pq", "")] {
        exec(
            &db,
            &format!(
                "CREATE TABLE {name} (tenant_id BIGINT NOT NULL, id BIGINT NOT NULL, n INT NOT NULL, s TEXT, \
                 amt NUMERIC(10,2), PRIMARY KEY (tenant_id, id)){tail}"
            ),
        )
        .await;
        let values: Vec<String> = (1..=12)
            .flat_map(|t| (1..=5).map(move |i| format!("({t}, {i}, {}, 's{t}-{i}', {t}.{i}5)", t * 10 + i)))
            .collect();
        exec(&db, &format!("INSERT INTO {name} (tenant_id, id, n, s, amt) VALUES {}", values.join(", "))).await;
        exec(&db, &format!("CREATE INDEX {name}_n ON {name} (n)")).await;
        exec(&db, &format!("UPDATE {name} SET n = n + 100 WHERE id = 2")).await;
        exec(&db, &format!("DELETE FROM {name} WHERE id = 5 AND tenant_id < 4")).await;
        let x = db.transactions.begin();
        exec_in(&db, x, &format!("INSERT INTO {name} (tenant_id, id, n, s) VALUES (3, 99, 0, 'aborted')")).await;
        db.abort_transaction(x);
    }
    db
}

fn uids_on_disk(dir: &Path) -> Vec<u64> {
    let mut uids: Vec<u64> = std::fs::read_dir(dir)
        .unwrap()
        .filter_map(|e| e.ok()?.file_name().into_string().ok())
        .filter_map(|n| files::parse_name(&n).map(|f: PartFile| f.uid()))
        .collect();
    uids.sort_unstable();
    uids.dedup();
    uids
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_physical_alter_answers_as_the_plain_twin() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().to_path_buf();
    let db = twins(&dir).await;
    compare(&db, "before").await;
    let first_uid = db.partitioned_table("pt").unwrap().uid();
    for alter in [
        "ALTER TABLE {t} ADD COLUMN extra INT",
        "ALTER TABLE {t} ADD COLUMN flag BOOLEAN DEFAULT true",
        "ALTER TABLE {t} ADD COLUMN label TEXT DEFAULT 'x'",
        // NUMERIC retypes rewrite every value (rounded to the new scale).
        "ALTER TABLE {t} ALTER COLUMN amt TYPE NUMERIC(12,1)",
        "ALTER TABLE {t} ALTER COLUMN amt TYPE NUMERIC",
        "ALTER TABLE {t} ALTER COLUMN n DROP NOT NULL",
        // Refused on both alike: a storage-type change is not built for any
        // table.
        "ALTER TABLE {t} ALTER COLUMN extra TYPE BIGINT",
    ] {
        let p = try_exec(&db, &alter.replace("{t}", "pt")).await.map(|_| ());
        let q = try_exec(&db, &alter.replace("{t}", "pq")).await.map(|_| ());
        assert_eq!(
            p.as_ref().map_err(|e| e.replace("pt", "{t}")),
            q.as_ref().map_err(|e| e.replace("pq", "{t}")),
            "{alter}"
        );
        compare(&db, alter).await;
        // The index survives the rewrite, and answers as a scan.
        same(&db, "SELECT tenant_id, id FROM {t} WHERE n = 112 ORDER BY tenant_id", alter).await;
    }
    // One table, one uid on disk: the old one is gone.
    let uid = db.partitioned_table("pt").unwrap().uid();
    assert_ne!(uid, first_uid, "the rewrite took a new uid");
    assert_eq!(uids_on_disk(&dir), vec![uid], "only the live uid has files");
    // Writes after, a NULL into the column now nullable.
    for w in [
        "INSERT INTO {t} (tenant_id, id, n, s, extra) VALUES (7, 50, NULL, 'after', 9)",
        "UPDATE {t} SET extra = extra + 1 WHERE tenant_id = 7",
        "DELETE FROM {t} WHERE tenant_id = 8",
    ] {
        exec(&db, &w.replace("{t}", "pt")).await;
        exec(&db, &w.replace("{t}", "pq")).await;
    }
    compare(&db, "after writes").await;
    close(&dir, db);
    let db = open(&dir).await;
    compare(&db, "after a restart").await;
    assert_eq!(uids_on_disk(&dir), vec![uid]);
    // A key column's type is still refused: it decides where rows live.
    let e = try_exec(&db, "ALTER TABLE pt ALTER COLUMN tenant_id TYPE INT").await.unwrap_err();
    assert!(e.contains("partition key column"), "{e}");
    close(&dir, db);
}

/// A failure before the commit point leaves the old table answering as
/// before, and no file of the new uid.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failed_rewrite_changes_nothing() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().to_path_buf();
    let db = twins(&dir).await;
    let uid = db.partitioned_table("pt").unwrap().uid();
    let before = exec(&db, "SELECT * FROM pt ORDER BY tenant_id, id").await;
    let guard = failpoint::arm(
        "test.rewrite.partitioned_before_commit",
        &dir,
        1,
        std::sync::Arc::new(|_| failpoint::Outcome::Fail(std::io::Error::other("injected failure"))),
    );
    let e = try_exec(&db, "ALTER TABLE pt ADD COLUMN extra INT").await.unwrap_err();
    assert!(e.contains("injected"), "{e}");
    assert_eq!(guard.fired(), 1);
    drop(guard);
    assert_eq!(db.partitioned_table("pt").unwrap().uid(), uid);
    assert_eq!(exec(&db, "SELECT * FROM pt ORDER BY tenant_id, id").await, before);
    assert_eq!(uids_on_disk(&dir), vec![uid], "the new uid's files are gone");
    // Writable, and the same ALTER then goes through.
    exec(&db, "INSERT INTO pt (tenant_id, id, n, s) VALUES (7, 70, 1, 'x')").await;
    exec(&db, "DELETE FROM pt WHERE tenant_id = 7 AND id = 70").await;
    exec(&db, "ALTER TABLE pt ADD COLUMN extra INT").await;
    close(&dir, db);
}

/// A crash right after the commit point: the next open reads the new uid,
/// and the startup reaper deletes the old one's files.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_crash_after_the_commit_point_opens_the_new_table() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().to_path_buf();
    let db = twins(&dir).await;
    let old_uid = db.partitioned_table("pt").unwrap().uid();
    exec(&db, "ALTER TABLE pq ADD COLUMN extra INT DEFAULT 4").await;
    let guard = failpoint::arm(
        "test.rewrite.partitioned_after_commit",
        &dir,
        1,
        std::sync::Arc::new(|_| failpoint::Outcome::Fail(std::io::Error::other("crash"))),
    );
    exec(&db, "ALTER TABLE pt ADD COLUMN extra INT DEFAULT 4").await;
    assert_eq!(guard.fired(), 1);
    drop(guard);
    assert!(uids_on_disk(&dir).contains(&old_uid), "the old files are left for the next open");
    close(&dir, db);
    let db = open(&dir).await;
    let uid = db.partitioned_table("pt").unwrap().uid();
    assert_ne!(uid, old_uid, "the catalogue names the new uid");
    compare(&db, "after the crash").await;
    assert_eq!(uids_on_disk(&dir), vec![uid], "the reaper deleted the old uid");
    close(&dir, db);
}
