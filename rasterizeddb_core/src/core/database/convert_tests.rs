//! Conversion of a plain table to a partitioned one (`partitioned_convert`):
//! the converted table answers exactly as a plain twin that was never
//! converted, every version carried (dead, aborted and its own
//! transaction's alike); a failure before the commit point leaves the plain
//! table; a crash after it opens the partitioned one with the plain files
//! gone; an intent left before the commit is abandoned at the next open.

use std::path::Path;

use rclite::Arc;

use super::Database;
use crate::core::partition::files::{self, PartFile};
use crate::core::power_loss::failpoint;

async fn open(dir: &Path) -> Arc<Database> {
    let _ = crate::MAX_PERMITS_THREADS.set(4);
    let _ = crate::BATCH_SIZE.set(512);
    let _ = crate::core::processor::concurrent_processor::ENABLE_CACHE.set(false);
    Arc::new(Database::try_new_named(&dir.to_string_lossy(), false, "convert").await.expect("open the database"))
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

async fn exec_in(db: &Arc<Database>, tx: crate::core::mvcc::transaction::TxId, sql: &str) -> Vec<u8> {
    let purpose = crate::core::rql::lexer_s1::recognize_query_purpose(sql).unwrap_or_else(|| panic!("cannot parse {sql}"));
    let out = Box::pin(crate::core::rql::executor::execute_with_context(purpose, db.clone(), 0, tx)).await;
    assert!(!out.is_empty() && out[0] != 3, "{sql}: {}", String::from_utf8_lossy(&out));
    out
}

/// `pt` (to be converted) and `pq` (its plain twin): the same rows, the same
/// updated, deleted and aborted versions, the same index.
async fn twins(dir: &Path) -> Arc<Database> {
    let db = open(dir).await;
    for name in ["pt", "pq"] {
        exec(
            &db,
            &format!(
                "CREATE TABLE {name} (tenant_id BIGINT NOT NULL, id BIGINT NOT NULL, n INT NOT NULL, s TEXT, \
                 amt NUMERIC(10,2), d INT DEFAULT 9, PRIMARY KEY (tenant_id, id))"
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

async fn compare(db: &Arc<Database>, at: &str) {
    for select in [
        "SELECT * FROM {t} ORDER BY tenant_id, id",
        "SELECT COUNT(*), SUM(id) FROM {t} WHERE tenant_id = 7",
        "SELECT tenant_id, COUNT(*) FROM {t} GROUP BY tenant_id ORDER BY tenant_id",
        "SELECT id FROM {t} WHERE n = 112 ORDER BY id",
    ] {
        let p = exec(db, &select.replace("{t}", "pt")).await;
        let q = exec(db, &select.replace("{t}", "pq")).await;
        assert_eq!(p, q, "{at}: `{select}` differs from the plain twin");
    }
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

/// Whether any file of a plain table named `name` is in `dir`.
fn plain_files(dir: &Path, name: &str) -> Vec<String> {
    std::fs::read_dir(dir)
        .unwrap()
        .filter_map(|e| e.ok()?.file_name().into_string().ok())
        .filter(|n| super::table_owns_file(name, n))
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_converted_table_answers_as_its_plain_twin() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().to_path_buf();
    let db = twins(&dir).await;
    exec(&db, "ALTER TABLE pt PARTITION BY HASH (tenant_id) PARTITIONS 6").await;
    assert!(db.partitioned_table("pt").is_some(), "pt is partitioned");
    assert!(plain_files(&dir, "pt").is_empty(), "plain files left: {:?}", plain_files(&dir, "pt"));
    assert!(!dir.join("__convert.intent").exists());
    compare(&db, "after the conversion").await;
    // Written through as a partitioned table: the default, the key, routing.
    for name in ["pt", "pq"] {
        exec(&db, &format!("INSERT INTO {name} (tenant_id, id, n, s) VALUES (7, 70, 1, 'x')")).await;
        assert!(try_exec(&db, &format!("INSERT INTO {name} (tenant_id, id, n) VALUES (7, 70, 2)")).await.is_err());
        exec(&db, &format!("UPDATE {name} SET s = 'y' WHERE tenant_id = 7 AND id = 70")).await;
    }
    compare(&db, "after writes").await;
    close(&dir, db);
    let db = open(&dir).await;
    assert!(db.partitioned_table("pt").is_some(), "pt is partitioned after a restart");
    compare(&db, "after a restart").await;
    close(&dir, db);
}

/// Inside a transaction: the transaction's own uncommitted row is carried,
/// visible to it after the conversion, and gone if it rolls back.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_conversion_keeps_its_own_transactions_rows() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().to_path_buf();
    let db = twins(&dir).await;
    for commit in [true, false] {
        let name = if commit { "pt" } else { "pq" };
        let x = db.transactions.begin();
        exec_in(&db, x, &format!("INSERT INTO {name} (tenant_id, id, n, s) VALUES (5, 500, 1, 'mine')")).await;
        exec_in(&db, x, &format!("ALTER TABLE {name} PARTITION BY HASH (tenant_id) PARTITIONS 4")).await;
        let seen = exec_in(&db, x, &format!("SELECT s FROM {name} WHERE tenant_id = 5 AND id = 500")).await;
        assert!(String::from_utf8_lossy(&seen).contains("mine"), "{name}: its own row");
        if commit {
            db.commit_transaction(x);
        } else {
            db.abort_transaction(x);
        }
        let after = exec(&db, &format!("SELECT COUNT(*) FROM {name} WHERE id = 500")).await;
        // One row and none, as the same COUNT over rows known to be there
        // and known not to be answers.
        let want = if commit {
            exec(&db, "SELECT COUNT(*) FROM pq WHERE tenant_id = 1 AND id = 1").await
        } else {
            exec(&db, "SELECT COUNT(*) FROM pq WHERE id = -1").await
        };
        assert_eq!(after, want, "{name}: the row after {}", if commit { "commit" } else { "rollback" });
    }
    close(&dir, db);
}

/// A failure before the commit point leaves the plain table as it was, with
/// no file of the new uid and no intent.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failed_conversion_changes_nothing() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().to_path_buf();
    let db = twins(&dir).await;
    let before = exec(&db, "SELECT * FROM pt ORDER BY tenant_id, id").await;
    for point in ["test.rewrite.convert_before_intent", "test.rewrite.convert_before_commit"] {
        let guard = failpoint::arm(
            point,
            &dir,
            1,
            std::sync::Arc::new(|_| failpoint::Outcome::Fail(std::io::Error::other("injected failure"))),
        );
        let e = try_exec(&db, "ALTER TABLE pt PARTITION BY HASH (tenant_id)").await.unwrap_err();
        assert!(e.contains("injected"), "{point}: {e}");
        assert_eq!(guard.fired(), 1, "{point}");
        drop(guard);
        assert!(db.partitioned_table("pt").is_none(), "{point}: pt is still plain");
        assert_eq!(exec(&db, "SELECT * FROM pt ORDER BY tenant_id, id").await, before, "{point}");
        assert!(uids_on_disk(&dir).is_empty(), "{point}: the new uid's files are gone");
        assert!(!dir.join("__convert.intent").exists(), "{point}: the intent is gone");
        exec(&db, "INSERT INTO pt (tenant_id, id, n) VALUES (7, 71, 1)").await;
        exec(&db, "DELETE FROM pt WHERE tenant_id = 7 AND id = 71").await;
    }
    exec(&db, "ALTER TABLE pt PARTITION BY HASH (tenant_id)").await;
    compare(&db, "converted after two failures").await;
    close(&dir, db);
}

/// A crash right after the commit point: the next open finds the
/// partitioned table, deletes the plain files the crash left and the
/// intent, and answers as the twin.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_crash_after_the_commit_point_finishes_at_the_next_open() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().to_path_buf();
    let db = twins(&dir).await;
    let guard = failpoint::arm(
        "test.rewrite.convert_after_commit",
        &dir,
        1,
        std::sync::Arc::new(|_| failpoint::Outcome::Fail(std::io::Error::other("crash"))),
    );
    exec(&db, "ALTER TABLE pt PARTITION BY HASH (tenant_id) PARTITIONS 5").await;
    assert_eq!(guard.fired(), 1);
    drop(guard);
    assert!(!plain_files(&dir, "pt").is_empty(), "the crash left the plain files");
    assert!(dir.join("__convert.intent").exists(), "and the intent");
    close(&dir, db);
    let db = open(&dir).await;
    assert!(db.partitioned_table("pt").is_some(), "the catalogue names the partitioned table");
    assert!(plain_files(&dir, "pt").is_empty(), "plain files left: {:?}", plain_files(&dir, "pt"));
    assert!(!dir.join("__convert.intent").exists());
    compare(&db, "after the crash").await;
    close(&dir, db);
}

/// An intent whose uid the catalogue does not name: the crash fell before
/// the commit point. The next open keeps the plain table, clears the intent,
/// and the reaper deletes the uid's files.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_intent_before_the_commit_point_is_abandoned() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().to_path_buf();
    let db = twins(&dir).await;
    let before = exec(&db, "SELECT * FROM pt ORDER BY tenant_id, id").await;
    close(&dir, db);
    let text = super::encode_intent("pt", "__pt_00000000000000ab");
    std::fs::write(dir.join("__convert.intent"), text).unwrap();
    let db = open(&dir).await;
    assert!(db.partitioned_table("pt").is_none());
    assert!(!dir.join("__convert.intent").exists(), "the intent is cleared");
    assert_eq!(exec(&db, "SELECT * FROM pt ORDER BY tenant_id, id").await, before);
    close(&dir, db);
}

/// Refused as a `CREATE … PARTITION BY` over the same definition is, and
/// the plain table left as it was, still writable: a key outside the
/// primary key, a nullable key, a table already partitioned.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_refused_conversion_leaves_the_plain_table() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().to_path_buf();
    let db = twins(&dir).await;
    exec(&db, "CREATE TABLE nk (id BIGINT PRIMARY KEY, t BIGINT)").await;
    exec(&db, "INSERT INTO nk VALUES (1, 1), (2, NULL)").await;
    let before = exec(&db, "SELECT * FROM pt ORDER BY tenant_id, id").await;
    for (sql, why) in [
        ("ALTER TABLE pt PARTITION BY HASH (n)", "does not contain partition key column"),
        ("ALTER TABLE nk PARTITION BY HASH (t)", "nullable"),
    ] {
        let e = try_exec(&db, sql).await.unwrap_err();
        assert!(e.contains(why), "{sql}: {e}");
    }
    assert!(db.partitioned_table("pt").is_none() && db.partitioned_table("nk").is_none());
    assert_eq!(exec(&db, "SELECT * FROM pt ORDER BY tenant_id, id").await, before);
    exec(&db, "INSERT INTO pt (tenant_id, id, n) VALUES (7, 72, 1)").await;
    exec(&db, "DELETE FROM pt WHERE tenant_id = 7 AND id = 72").await;
    assert!(uids_on_disk(&dir).is_empty(), "a refusal wrote no uid");
    exec(&db, "ALTER TABLE pt PARTITION BY HASH (tenant_id)").await;
    let e = try_exec(&db, "ALTER TABLE pt PARTITION BY HASH (tenant_id)").await.unwrap_err();
    assert!(e.contains("already partitioned"), "{e}");
    compare(&db, "after the refusals").await;
    close(&dir, db);
}
