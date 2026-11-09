//! A block's DDL across a crash (`block_ddl`): a block that did not commit
//! has its ALTER undone and its CREATE dropped at the next open; one whose
//! commit is recorded keeps both, though it crashed before its copies were
//! cleaned up.

use std::path::Path;

use rclite::Arc;

use super::relation_dispatch_tests::open;
use super::*;

fn shut(db: Arc<Database>, root: &Path) {
    drop(db);
    crate::core::wal::remove_for(&root.to_string_lossy());
}

async fn exec(db: &Arc<Database>, sql: &str) -> Result<Vec<u8>, String> {
    let purpose = crate::core::rql::lexer_s1::recognize_query_purpose(sql).ok_or_else(|| format!("cannot parse {sql}"))?;
    let out = crate::core::rql::executor::execute_with_sync(purpose, db.clone(), true).await;
    if out.is_empty() || out[0] == 3 {
        return Err(format!("{sql}: {}", String::from_utf8_lossy(out.get(1..).unwrap_or_default())));
    }
    Ok(out)
}

/// Runs `statements` as block `root`'s DDL, as pgwire does: prepared, then
/// executed under the block's owner.
async fn block_ddl(db: &Arc<Database>, root: crate::core::mvcc::TxId, statements: &[&str]) {
    for sql in statements {
        let db2 = db.clone();
        let sql = sql.to_string();
        crate::core::mvcc::owner::under(&db.transactions, root, crate::core::mvcc::owner::OwnerKind::Block, async move {
            assert!(db2.block_ddl_prepare(root, &sql).await.expect("prepare"), "{sql} is transactional");
            let purpose = crate::core::rql::lexer_s1::recognize_query_purpose(&sql).expect("parse");
            let out = crate::core::rql::executor::execute_in_txn(purpose, db2.clone(), root).await;
            assert_ne!(out.first(), Some(&3), "{sql}: {}", String::from_utf8_lossy(&out[1..]));
        })
        .await;
    }
}

fn columns(db: &Database, table: &str) -> Vec<String> {
    db.schema_view(table)
        .map(|v| v.schema().fields.iter().filter(|f| !f.is_deleted && !f.name.starts_with("__")).map(|f| f.name.clone()).collect())
        .unwrap_or_default()
}

async fn ids(db: &Arc<Database>, table: &str) -> usize {
    let out = exec(db, &format!("SELECT id FROM {table}")).await.expect("select");
    crate::core::row::row::vec_into_rows(&out[1..]).expect("rows").len()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_crash_before_commit_undoes_the_blocks_ddl() {
    let dir = tempfile::tempdir().expect("dir");
    let db = Arc::new(open(dir.path()).await);
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, name TEXT)").await.expect("create");
    exec(&db, "INSERT INTO t (id, name) VALUES (1, 'a'), (2, 'b')").await.expect("insert");
    let root = db.transactions.begin();
    block_ddl(&db, root, &["ALTER TABLE t ADD COLUMN extra INT", "CREATE TABLE fresh (id INT PRIMARY KEY)"]).await;
    assert!(columns(&db, "t").contains(&"extra".to_string()));
    assert!(db.relation("fresh").is_some());
    // The crash: no commit, no abort.
    shut(db, dir.path());

    let db = Arc::new(open(dir.path()).await);
    assert_eq!(columns(&db, "t"), vec!["id".to_string(), "name".to_string()], "the ALTER survived the crash");
    assert_eq!(ids(&db, "t").await, 2, "the rows from before the block");
    assert!(db.relation("fresh").is_none(), "the created table survived the crash");
    exec(&db, "CREATE TABLE fresh (id INT PRIMARY KEY)").await.expect("the name is free again");
    let leftovers: Vec<String> = std::fs::read_dir(dir.path())
        .expect("dir")
        .filter_map(|e| e.ok().and_then(|e| e.file_name().into_string().ok()))
        .filter(|n| n.starts_with("__txddl_") || n.starts_with(block_ddl::COPY_PREFIX))
        .collect();
    assert!(leftovers.is_empty(), "left behind: {leftovers:?}");
    shut(db, dir.path());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_crash_after_a_recorded_commit_keeps_the_blocks_ddl() {
    let dir = tempfile::tempdir().expect("dir");
    let db = Arc::new(open(dir.path()).await);
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, name TEXT)").await.expect("create");
    exec(&db, "INSERT INTO t (id, name) VALUES (1, 'a')").await.expect("insert");
    let root = db.transactions.begin();
    block_ddl(&db, root, &["ALTER TABLE t ADD COLUMN extra INT", "CREATE TABLE fresh (id INT PRIMARY KEY)"]).await;
    // The commit made durable, as `commit_block` does, and then the crash,
    // before the block's copies were discarded.
    db.commit_written_transaction(root).expect("commit");
    db.transactions.flush_deferred().expect("outcome");
    db.sync_own_location().expect("sync");
    shut(db, dir.path());

    let db = Arc::new(open(dir.path()).await);
    assert!(columns(&db, "t").contains(&"extra".to_string()), "a committed ALTER was undone");
    assert!(db.relation("fresh").is_some(), "a committed CREATE was dropped");
    assert_eq!(ids(&db, "t").await, 1);
    let leftovers: Vec<String> = std::fs::read_dir(dir.path())
        .expect("dir")
        .filter_map(|e| e.ok().and_then(|e| e.file_name().into_string().ok()))
        .filter(|n| n.starts_with("__txddl_") || n.starts_with(block_ddl::COPY_PREFIX))
        .collect();
    assert!(leftovers.is_empty(), "left behind: {leftovers:?}");
    shut(db, dir.path());
}

/// A table on a second storage location: its copy goes beside it (the
/// placement carried first, as the rewrite's backup does), so the block's
/// ROLLBACK and a crash before its commit both put back the rows it had,
/// and nothing of the copy is left in either directory.
///
/// Guards against the copy's provider resolved before its placement is
/// recorded (the copy written to the root, then looked for beside the table).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_table_on_a_second_location_rolls_back_and_recovers() {
    for crash in [false, true] {
        let dir = tempfile::tempdir().expect("dir");
        let (root_dir, disk) = (dir.path().join("root"), dir.path().join("disk"));
        std::fs::create_dir_all(&root_dir).expect("root");
        std::fs::create_dir_all(&disk).expect("disk");
        let db = Arc::new(open(&root_dir).await);
        let disk_s = disk.to_string_lossy().into_owned();
        db.storage.add_location(&disk_s, crate::core::storage::LocationRole::Primary, 1, true).expect("location");
        db.storage.pin("far", &disk_s).expect("pin");
        exec(&db, "CREATE TABLE far (id INT PRIMARY KEY, name TEXT)").await.expect("create");
        exec(&db, "INSERT INTO far (id, name) VALUES (1, 'a'), (2, 'b'), (3, 'c')").await.expect("insert");
        assert_eq!(db.storage.location_of("far").as_deref(), Some(disk_s.as_str()), "the table is not on the second location");
        let root = db.transactions.begin();
        block_ddl(&db, root, &["ALTER TABLE far ADD COLUMN extra INT DEFAULT 5"]).await;
        assert!(columns(&db, "far").contains(&"extra".to_string()));
        let db = if crash {
            shut(db, &root_dir);
            Arc::new(open(&root_dir).await)
        } else {
            db.block_ddl_abort(root).await;
            db.abort_transaction(root);
            db
        };
        assert_eq!(columns(&db, "far"), vec!["id".to_string(), "name".to_string()], "crash {crash}: the ALTER stayed");
        assert_eq!(ids(&db, "far").await, 3, "crash {crash}: the rows from before the block");
        for d in [&root_dir, &disk] {
            let leftovers: Vec<String> = std::fs::read_dir(d)
                .expect("dir")
                .filter_map(|e| e.ok().and_then(|e| e.file_name().into_string().ok()))
                .filter(|n| n.starts_with("__txddl_") || n.starts_with(block_ddl::COPY_PREFIX))
                .collect();
            assert!(leftovers.is_empty(), "crash {crash}: left behind in {}: {leftovers:?}", d.display());
        }
        shut(db, &root_dir);
    }
}
