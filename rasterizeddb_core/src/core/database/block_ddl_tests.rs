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

/// How a block ends, for the tests below.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum End {
    /// ROLLBACK, as pgwire's abort runs it.
    Rollback,
    /// No commit and no abort: the process stops and the next open recovers.
    Crash,
    /// A commit made durable, as `commit_block` does.
    Commit,
}

async fn end_block(db: Arc<Database>, root: crate::core::mvcc::TxId, end: End, dir: &Path) -> Arc<Database> {
    let _ = env_logger::builder().is_test(true).try_init();
    // The block's owner gives its table locks back as pgwire's does when the
    // block ends (`OwnerRelease`): kept, the next statement on the table
    // waits for them forever.
    let owner = crate::core::mvcc::owner::OwnerRelease::for_root(&db.row_locks, root);
    match end {
        End::Rollback => {
            db.block_ddl_abort(root).await;
            db.abort_transaction(root);
            owner.release();
            db
        }
        End::Crash => {
            shut(db, dir);
            Arc::new(open(dir).await)
        }
        End::Commit => {
            db.commit_written_transaction(root).expect("commit");
            db.transactions.flush_deferred().expect("outcome");
            db.sync_own_location().expect("sync");
            db.block_ddl_commit(root).await;
            owner.release();
            db
        }
    }
}

/// The live index declarations of `table`, sorted: column and path indexes,
/// and partial unique keys.
fn index_names(db: &Database, table: &str) -> Vec<String> {
    let mut names: Vec<String> = db
        .schema_view(table)
        .map(|v| {
            let s = v.schema();
            s.secondary_indexes
                .iter()
                .filter(|i| !i.is_deleted)
                .map(|i| i.name.clone())
                .chain(s.path_indexes.iter().filter(|i| !i.is_deleted).map(|i| i.name.clone()))
                .chain(s.unique_keys.iter().filter(|k| !k.is_deleted && k.predicate.is_some()).map(|k| k.name.clone()))
                .collect()
        })
        .unwrap_or_default();
    names.sort();
    names
}

/// [`block_leftovers`], waited for up to ten seconds: a key/value store
/// erases a discarded file's objects in a task of its own.
async fn settled_leftovers(dir: &Path) -> Vec<String> {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        let found = block_leftovers(dir);
        if found.is_empty() || std::time::Instant::now() > deadline {
            return found;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
}

/// Every file or object under `dir`, recursively, whose path says it is a
/// block's copy or intent. Files only: a directory store keeps a file's
/// objects under a directory named after it, which its erase empties and
/// leaves.
fn block_leftovers(dir: &Path) -> Vec<String> {
    let mut found = Vec::new();
    let mut stack = vec![dir.to_path_buf()];
    while let Some(d) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&d) else { continue };
        for e in entries.flatten() {
            let path = e.path();
            if path.is_dir() {
                stack.push(path.clone());
                continue;
            }
            let shown = path.strip_prefix(dir).unwrap_or(&path).to_string_lossy().into_owned();
            if shown.contains("__txddl_") || shown.contains(block_ddl::COPY_PREFIX) {
                found.push(path.display().to_string());
            }
        }
    }
    found
}

/// `CREATE INDEX` and `DROP INDEX` in a block (2026-11-12): only the
/// table's declarations are held aside (its schema files), and a ROLLBACK
/// or a crash before COMMIT puts them back; a COMMIT keeps the change. Both
/// used to apply at once and outlive the ROLLBACK, with a warning.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn index_ddl_in_a_block_rolls_back_recovers_and_commits() {
    for end in [End::Rollback, End::Crash, End::Commit] {
        let dir = tempfile::tempdir().expect("dir");
        let db = Arc::new(open(dir.path()).await);
        exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, x INT, name TEXT, doc JSONB)").await.expect("create");
        exec(&db, "CREATE INDEX t_x ON t (x)").await.expect("index");
        exec(&db, "CREATE INDEX t_doc_k ON t ((doc->>'k'))").await.expect("path index");
        exec(&db, "INSERT INTO t (id, x, name, doc) VALUES (1, 10, 'a', '{\"k\":\"p\"}'), (2, 20, 'b', '{\"k\":\"q\"}')")
            .await
            .expect("insert");
        assert_eq!(index_names(&db, "t"), vec!["t_doc_k".to_string(), "t_x".to_string()]);
        let root = db.transactions.begin();
        // The drops first: each must hold the declarations aside itself,
        // which a CREATE INDEX before them would do for them.
        block_ddl(&db, root, &["DROP INDEX t_x", "DROP INDEX t_doc_k", "CREATE INDEX t_name ON t (name)"]).await;
        assert_eq!(index_names(&db, "t"), vec!["t_name".to_string()], "{end:?}: the block's DDL did not run");
        let db = end_block(db, root, end, dir.path()).await;
        let expected: Vec<String> = match end {
            End::Commit => vec!["t_name".to_string()],
            _ => vec!["t_doc_k".to_string(), "t_x".to_string()],
        };
        assert_eq!(index_names(&db, "t"), expected, "{end:?}");
        // The indexes answer: a lookup through each finds its row.
        let out = exec(&db, "SELECT id FROM t WHERE x = 20").await.expect("by x");
        assert_eq!(crate::core::row::row::vec_into_rows(&out[1..]).expect("rows").len(), 1, "{end:?}: by x");
        let out = exec(&db, "SELECT id FROM t WHERE doc->>'k' = 'p'").await.expect("by path");
        assert_eq!(crate::core::row::row::vec_into_rows(&out[1..]).expect("rows").len(), 1, "{end:?}: by path");
        assert_eq!(ids(&db, "t").await, 2, "{end:?}");
        assert!(block_leftovers(dir.path()).is_empty(), "{end:?}: left behind: {:?}", block_leftovers(dir.path()));
        shut(db, dir.path());
    }
}

/// A partitioned table's DDL in a block (2026-11-12): a metadata ALTER and
/// a CREATE INDEX hold its shell's schema files aside (`__pt_<uid>_
/// schema*`, all such DDL writes), and a table created PARTITION BY is
/// dropped; all three applied at once before and outlived a ROLLBACK. (The
/// table has no rows yet, so its ALTER changes only metadata; one that
/// rewrites rows is the next test.)
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_partitioned_tables_ddl_in_a_block_rolls_back_recovers_and_commits() {
    for end in [End::Rollback, End::Crash, End::Commit] {
        let dir = tempfile::tempdir().expect("dir");
        let db = Arc::new(open(dir.path()).await);
        exec(&db, "CREATE TABLE p (id INT NOT NULL, v TEXT) PARTITION BY HASH (id)").await.expect("create");
        let root = db.transactions.begin();
        block_ddl(
            &db,
            root,
            &[
                "ALTER TABLE p ADD COLUMN w INT",
                "CREATE INDEX p_v ON p (v)",
                "CREATE TABLE q (id INT NOT NULL) PARTITION BY HASH (id)",
            ],
        )
        .await;
        assert!(columns(&db, "p").contains(&"w".to_string()), "{end:?}: the ALTER did not run");
        assert_eq!(index_names(&db, "p"), vec!["p_v".to_string()], "{end:?}: the index was not declared");
        let db = end_block(db, root, end, dir.path()).await;
        match end {
            End::Commit => {
                assert_eq!(columns(&db, "p"), vec!["id".to_string(), "v".to_string(), "w".to_string()]);
                assert_eq!(index_names(&db, "p"), vec!["p_v".to_string()]);
                assert!(db.is_partitioned("q"), "the committed CREATE was dropped");
            }
            _ => {
                assert_eq!(columns(&db, "p"), vec!["id".to_string(), "v".to_string()], "{end:?}: the ALTER stayed");
                assert!(index_names(&db, "p").is_empty(), "{end:?}: the index stayed");
                assert!(db.relation("q").is_none(), "{end:?}: the created table stayed");
            }
        }
        exec(&db, "INSERT INTO p (id, v) VALUES (1, 'a'), (2, 'b'), (3, 'c')").await.expect("the table takes rows");
        assert_eq!(ids(&db, "p").await, 3, "{end:?}");
        assert!(block_leftovers(dir.path()).is_empty(), "{end:?}: left behind: {:?}", block_leftovers(dir.path()));
        shut(db, dir.path());
    }
}

/// A table on a key/value store (2026-11-12): its objects are held aside on
/// the store, through a seed that refuses rather than copy an unreachable
/// store's empty stand-in, and put back by a ROLLBACK or the next open.
/// Before, a block's DDL on one applied at once, with a warning.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_key_value_tables_ddl_in_a_block_rolls_back_recovers_and_commits() {
    for end in [End::Rollback, End::Crash, End::Commit] {
        let dir = tempfile::tempdir().expect("dir");
        let store = tempfile::tempdir().expect("store");
        let db = Arc::new(open(dir.path()).await);
        crate::core::storage::sql::execute(
            &format!(
                "STORAGE ADD STORE fast DIRECTORY WITH (PATH = '{}', PREFIX = 'rdb', CHUNK_SIZE = '1MiB', CACHE_BYTES = '8MiB')",
                store.path().to_string_lossy()
            ),
            &db,
        )
        .await
        .expect("store");
        crate::core::storage::sql::execute("STORAGE PLACE TABLE k AT 'kv:fast'", &db).await.expect("place");
        exec(&db, "CREATE TABLE k (id INT PRIMARY KEY, name TEXT)").await.expect("create");
        exec(&db, "INSERT INTO k (id, name) VALUES (1, 'a'), (2, 'b'), (3, 'c')").await.expect("insert");
        let root = db.transactions.begin();
        block_ddl(&db, root, &["ALTER TABLE k ADD COLUMN extra INT DEFAULT 5", "CREATE INDEX k_name ON k (name)"]).await;
        assert!(columns(&db, "k").contains(&"extra".to_string()), "{end:?}: the ALTER did not run");
        let db = end_block(db, root, end, dir.path()).await;
        match end {
            End::Commit => {
                assert_eq!(columns(&db, "k"), vec!["id".to_string(), "name".to_string(), "extra".to_string()]);
                assert_eq!(index_names(&db, "k"), vec!["k_name".to_string()]);
            }
            _ => {
                assert_eq!(columns(&db, "k"), vec!["id".to_string(), "name".to_string()], "{end:?}: the ALTER stayed");
                assert!(index_names(&db, "k").is_empty(), "{end:?}: the index stayed");
            }
        }
        assert_eq!(ids(&db, "k").await, 3, "{end:?}: the rows");
        for d in [dir.path(), store.path()] {
            let left = settled_leftovers(d).await;
            assert!(left.is_empty(), "{end:?}: left behind: {left:?}");
        }
        shut(db, dir.path());
    }
}


/// An ALTER that rewrites a partitioned table's rows moves them to a new uid
/// and deletes the old one's files (phase 6 P8), so the block's copy of the
/// old schema cannot go back. The step is forgotten when the table moves
/// (`block_ddl_table_moved`, and the client is warned): a ROLLBACK leaves
/// the rewrite in place rather than put the copy back as files no table
/// owns and report the change undone. Its earlier DDL on the table goes
/// with it; the rows are the table's either way.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_partitioned_rewrite_in_a_block_is_not_put_back_as_orphan_files() {
    for end in [End::Rollback, End::Crash] {
        let dir = tempfile::tempdir().expect("dir");
        let db = Arc::new(open(dir.path()).await);
        exec(&db, "CREATE TABLE p (id INT NOT NULL, v TEXT) PARTITION BY HASH (id)").await.expect("create");
        exec(&db, "INSERT INTO p (id, v) VALUES (1, 'a'), (2, 'b'), (3, 'c')").await.expect("insert");
        let before = db.schema_files_stem("p");
        let root = db.transactions.begin();
        block_ddl(&db, root, &["CREATE INDEX p_v ON p (v)", "ALTER TABLE p ADD COLUMN w INT"]).await;
        assert_ne!(db.schema_files_stem("p"), before, "the ALTER did not rewrite into a new uid; this test needs one that does");
        assert_eq!(db.block_ddl_len(root), 0, "{end:?}: a step for the old uid's files was kept");
        let db = end_block(db, root, end, dir.path()).await;
        assert!(columns(&db, "p").contains(&"w".to_string()), "{end:?}: the rewrite is the table now");
        assert_eq!(ids(&db, "p").await, 3, "{end:?}: the rows");
        let orphans: Vec<String> = std::fs::read_dir(dir.path())
            .expect("dir")
            .flatten()
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .filter(|n| n.starts_with(&before))
            .collect();
        assert!(orphans.is_empty(), "{end:?}: the old uid's files came back: {orphans:?}");
        assert!(block_leftovers(dir.path()).is_empty(), "{end:?}: left behind: {:?}", block_leftovers(dir.path()));
        shut(db, dir.path());
    }
}
