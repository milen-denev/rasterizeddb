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
    /// The commit made durable, then the process stops before the block's
    /// deferred drops run: the next open finishes them.
    CommitCrash,
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
            // The deferred drops, once durable, as `commit_block` runs them.
            for drop in db.block_drops_take(root, None) {
                let db2 = db.clone();
                let sql = drop.sql.clone();
                let out = crate::core::mvcc::owner::under(&db.transactions, root, crate::core::mvcc::owner::OwnerKind::Block, async move {
                    let purpose = crate::core::rql::lexer_s1::recognize_query_purpose(&sql).expect("parse");
                    crate::core::rql::executor::execute_in_txn(purpose, db2, root).await
                })
                .await;
                assert_ne!(out.first(), Some(&3), "{}: {}", drop.sql, String::from_utf8_lossy(&out[1..]));
                drop.done();
            }
            db.block_ddl_commit(root).await;
            owner.release();
            db
        }
        End::CommitCrash => {
            db.commit_written_transaction(root).expect("commit");
            db.transactions.flush_deferred().expect("outcome");
            db.sync_own_location().expect("sync");
            shut(db, dir);
            Arc::new(open(dir).await)
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


/// The files directly under `dir` (not the paged index runs, `.idxr`
/// directories, which go when their last reader lets go) of partitioned
/// table stem `stem` (`__pt_<uid>`).
fn uid_files(dir: &Path, stem: &str) -> Vec<String> {
    std::fs::read_dir(dir)
        .expect("dir")
        .flatten()
        .filter(|e| e.path().is_file())
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .filter(|n| n.starts_with(&format!("{stem}_")))
        .collect()
}

/// An ALTER that rewrites a partitioned table's rows moves them to a new uid
/// (phase 6 P8). In a block (2026-11-12) the old uid's files are kept until
/// the block ends: a ROLLBACK, or a crash before COMMIT, moves the
/// catalogue row back, reopens the old table and deletes the new uid; a
/// COMMIT deletes the old uid (or, after a crash past the commit, the
/// startup reaper does). Before, the rewrite deleted the old files at once
/// and a ROLLBACK left the rewrite in place, with a warning.
///
/// Two rewrites in one block, with the block's own row and an index between
/// them, so an undo walks two moves back through a schema restore.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_partitioned_rewrite_in_a_block_rolls_back_recovers_and_commits() {
    for end in [End::Rollback, End::Crash, End::Commit, End::CommitCrash] {
        let dir = tempfile::tempdir().expect("dir");
        let db = Arc::new(open(dir.path()).await);
        exec(&db, "CREATE TABLE p (id INT NOT NULL, v TEXT) PARTITION BY HASH (id)").await.expect("create");
        exec(&db, "INSERT INTO p (id, v) VALUES (1, 'a'), (2, 'b'), (3, 'c')").await.expect("insert");
        let before = db.schema_files_stem("p");
        let root = db.transactions.begin();
        block_ddl(&db, root, &["CREATE INDEX p_v ON p (v)", "ALTER TABLE p ADD COLUMN w INT DEFAULT 7"]).await;
        let first = db.schema_files_stem("p");
        assert_ne!(first, before, "the ALTER did not rewrite into a new uid; this test needs one that does");
        assert!(!uid_files(dir.path(), &before).is_empty(), "{end:?}: the old uid's files went before the block ended");
        in_block(&db, root, "INSERT INTO p (id, v, w) VALUES (4, 'd', 8)").await.expect("the block's row");
        block_ddl(&db, root, &["ALTER TABLE p ADD COLUMN x INT DEFAULT 9"]).await;
        let second = db.schema_files_stem("p");
        assert_ne!(second, first, "the second ALTER did not rewrite");
        // Read in the block: it holds the table's ACCESS EXCLUSIVE.
        let out = in_block(&db, root, "SELECT id FROM p WHERE x = 9").await.expect("read in the block");
        assert_eq!(crate::core::row::row::vec_into_rows(&out[1..]).expect("rows").len(), 4, "{end:?}: the rewrite's rows");
        let db = end_block(db, root, end, dir.path()).await;
        let committed = matches!(end, End::Commit | End::CommitCrash);
        if committed {
            assert_eq!(columns(&db, "p"), ["id", "v", "w", "x"], "{end:?}");
            assert_eq!(index_names(&db, "p"), ["p_v"], "{end:?}");
            assert_eq!(db.schema_files_stem("p"), second, "{end:?}");
            assert_eq!(ids(&db, "p").await, 4, "{end:?}: the rows, the block's included");
            assert_eq!(count(&db, "SELECT id FROM p WHERE w = 7").await, 3, "{end:?}: the default the rewrite wrote");
            for gone in [&before, &first] {
                assert!(uid_files(dir.path(), gone).is_empty(), "{end:?}: {gone} kept: {:?}", uid_files(dir.path(), gone));
            }
        } else {
            assert_eq!(columns(&db, "p"), ["id", "v"], "{end:?}: the rewrite stayed");
            assert!(index_names(&db, "p").is_empty(), "{end:?}: the index stayed");
            assert_eq!(db.schema_files_stem("p"), before, "{end:?}: the table is not the one before the block");
            assert_eq!(ids(&db, "p").await, 3, "{end:?}: the rows before the block, without the block's");
            for gone in [&first, &second] {
                assert!(uid_files(dir.path(), gone).is_empty(), "{end:?}: {gone} kept: {:?}", uid_files(dir.path(), gone));
            }
        }
        exec(&db, "INSERT INTO p (id, v) VALUES (10, 'z')").await.expect("the table takes rows");
        exec(&db, "UPDATE p SET v = 'y' WHERE id = 1").await.expect("and updates");
        assert_eq!(count(&db, "SELECT id FROM p WHERE v = 'y'").await, 1, "{end:?}");
        assert!(block_leftovers(dir.path()).is_empty(), "{end:?}: left behind: {:?}", block_leftovers(dir.path()));
        // And it survives the next open as it is.
        let stem = db.schema_files_stem("p");
        shut(db, dir.path());
        let db = Arc::new(open(dir.path()).await);
        assert_eq!(db.schema_files_stem("p"), stem, "{end:?}: reopened");
        assert_eq!(ids(&db, "p").await, if committed { 5 } else { 4 }, "{end:?}: reopened");
        shut(db, dir.path());
    }
}

/// A crash's undo of a block's partitioned rewrite that cannot move the
/// catalogue row back (an I/O failure, injected) keeps the old uid's files
/// from the startup reaper and keeps the intent; the table, left at the
/// uncommitted rewrite's uid, opens refused, so no write lands where the
/// next start's retry deletes it. That retry puts the table back; or, once
/// the refused table is dropped, the retry finds no row and the next start
/// leaves nothing of either uid.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_rewrite_undo_that_fails_at_startup_refuses_the_table_and_retries() {
    for drop_it in [false, true] {
        let dir = tempfile::tempdir().expect("dir");
        let db = Arc::new(open(dir.path()).await);
        exec(&db, "CREATE TABLE p (id INT NOT NULL, v TEXT) PARTITION BY HASH (id)").await.expect("create");
        exec(&db, "INSERT INTO p (id, v) VALUES (1, 'a'), (2, 'b'), (3, 'c')").await.expect("insert");
        let before = db.schema_files_stem("p");
        let root = db.transactions.begin();
        block_ddl(&db, root, &["ALTER TABLE p ADD COLUMN w INT DEFAULT 7"]).await;
        let rewritten = db.schema_files_stem("p");
        assert_ne!(rewritten, before, "the ALTER did not rewrite into a new uid; this test needs one that does");

        block_ddl::swap_back_hooks::fail_recovery_swap_back(dir.path());
        let db = end_block(db, root, End::Crash, dir.path()).await;
        assert!(!uid_files(dir.path(), &before).is_empty(), "drop_it={drop_it}: the reaper took the old uid the retry needs");
        assert!(!uid_files(dir.path(), &rewritten).is_empty(), "drop_it={drop_it}: the table's current files went");
        assert!(!block_leftovers(dir.path()).is_empty(), "drop_it={drop_it}: the intent the retry needs went");
        // Every statement fails; the ones that need no column name say why
        // (a named column fails first against the refused table's empty
        // stand-in schema, as for any refused partitioned table).
        for sql in ["SELECT * FROM p", "SELECT COUNT(*) FROM p"] {
            let err = exec(&db, sql).await.expect_err("the half-recovered table answered");
            assert!(err.contains("could not put back"), "drop_it={drop_it}: {sql}: {err}");
        }
        for sql in [
            "SELECT id FROM p",
            "INSERT INTO p (id, v) VALUES (9, 'x')",
            "INSERT INTO p VALUES (9, 'x')",
            "UPDATE p SET v = 'q'",
            "DELETE FROM p",
        ] {
            assert!(exec(&db, sql).await.is_err(), "drop_it={drop_it}: {sql} reached the half-recovered table");
        }
        if drop_it {
            exec(&db, "DROP TABLE p").await.expect("a refused table can still be dropped");
        }
        shut(db, dir.path());

        block_ddl::swap_back_hooks::disarm(dir.path());
        let db = Arc::new(open(dir.path()).await);
        if drop_it {
            assert!(exec(&db, "SELECT id FROM p").await.is_err(), "the dropped table came back");
            assert!(uid_files(dir.path(), &before).is_empty(), "the old uid outlived its dropped table");
        } else {
            assert_eq!(db.schema_files_stem("p"), before, "the retry did not put the table back");
            assert_eq!(columns(&db, "p"), ["id", "v"]);
            assert_eq!(ids(&db, "p").await, 3);
            exec(&db, "INSERT INTO p (id, v) VALUES (10, 'z')").await.expect("the table takes rows");
            assert_eq!(ids(&db, "p").await, 4);
        }
        assert!(uid_files(dir.path(), &rewritten).is_empty(), "drop_it={drop_it}: the rewrite's uid kept");
        assert!(block_leftovers(dir.path()).is_empty(), "drop_it={drop_it}: left behind: {:?}", block_leftovers(dir.path()));
        shut(db, dir.path());
    }
}

/// Block `root`'s `DROP TABLE` as pgwire runs it: deferred to the commit.
async fn block_drop(db: &Arc<Database>, root: crate::core::mvcc::TxId, table: &str, sql: &str) {
    let (db2, table, sql) = (db.clone(), table.to_string(), sql.to_string());
    crate::core::mvcc::owner::under(&db.transactions, root, crate::core::mvcc::owner::OwnerKind::Block, async move {
        db2.block_drop_defer(root, vec![(table, sql)]).await.expect("defer");
    })
    .await;
}

/// Block `root`'s `CREATE TABLE` of a name it dropped, as pgwire runs it:
/// the dropped table set aside first.
async fn block_create_over(db: &Arc<Database>, root: crate::core::mvcc::TxId, name: &str, sql: &str) {
    let (db2, name) = (db.clone(), name.to_string());
    crate::core::mvcc::owner::under(&db.transactions, root, crate::core::mvcc::owner::OwnerKind::Block, async move {
        block_ddl::set_aside(&db2, root, &name).await.expect("set aside");
    })
    .await;
    block_ddl(db, root, &[sql]).await;
}

/// A statement of block `root` (DML), under its owner.
async fn in_block(db: &Arc<Database>, root: crate::core::mvcc::TxId, sql: &str) -> Result<Vec<u8>, String> {
    let (db2, sql) = (db.clone(), sql.to_string());
    crate::core::mvcc::owner::under(&db.transactions, root, crate::core::mvcc::owner::OwnerKind::Block, async move {
        let purpose = crate::core::rql::lexer_s1::recognize_query_purpose(&sql).ok_or("parse")?;
        let out = crate::core::rql::executor::execute_in_txn(purpose, db2, root).await;
        match out.first() {
            Some(&3) => Err(String::from_utf8_lossy(&out[1..]).into_owned()),
            _ => Ok(out),
        }
    })
    .await
}

async fn count(db: &Arc<Database>, sql: &str) -> usize {
    let out = exec(db, sql).await.expect("select");
    crate::core::row::row::vec_into_rows(&out[1..]).expect("rows").len()
}

/// The hidden tables a block set aside, in the catalogue or on disk.
fn asides(db: &Database, dir: &Path) -> Vec<String> {
    let mut found = Vec::new();
    db.each_schema(|t, _| {
        if t.starts_with(block_ddl::ASIDE_PREFIX) {
            found.push(t.to_string());
        }
    });
    // Its files, not its paged index runs' directory (`<stem>.idxr`), which
    // the store empties when its last reader lets go and leaves, as it does
    // for any table renamed or dropped under disk residency.
    for e in std::fs::read_dir(dir).expect("dir").flatten() {
        let n = e.file_name().to_string_lossy().into_owned();
        if n.starts_with(block_ddl::ASIDE_PREFIX) && !n.ends_with(".idxr") {
            found.push(n);
        }
    }
    found
}

/// `ALTER TABLE … RENAME TO` in a block (2026-11-12): a ROLLBACK, or a
/// crash before COMMIT, renames it back, and puts back what the rename's
/// fixups rewrote: its `SERIAL` default, its own foreign key and the one a
/// child holds on it. Its counter seeds again from its rows, so the next
/// key is not one it has. It used to apply at once and outlive the ROLLBACK.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_rename_in_a_block_rolls_back_recovers_and_commits() {
    for end in [End::Rollback, End::Crash, End::Commit] {
        let dir = tempfile::tempdir().expect("dir");
        let db = Arc::new(open(dir.path()).await);
        for sql in [
            "CREATE TABLE t (id SERIAL PRIMARY KEY, parent INT REFERENCES t(id), name TEXT)",
            "CREATE TABLE c (id INT PRIMARY KEY, t_id INT REFERENCES t(id))",
            "INSERT INTO t (parent, name) VALUES (NULL, 'a')",
            "INSERT INTO t (parent, name) VALUES (1, 'b')",
            "INSERT INTO t (parent, name) VALUES (1, 'c')",
            "INSERT INTO c (id, t_id) VALUES (1, 1)",
        ] {
            exec(&db, sql).await.expect(sql);
        }
        let root = db.transactions.begin();
        block_ddl(&db, root, &["ALTER TABLE t RENAME TO u"]).await;
        assert!(db.relation("u").is_some() && db.relation("t").is_none(), "{end:?}: the rename did not run");
        let db = end_block(db, root, end, dir.path()).await;
        let (kept, gone) = if end == End::Commit { ("u", "t") } else { ("t", "u") };
        assert!(db.relation(kept).is_some() && db.relation(gone).is_none(), "{end:?}: {kept} should be the table");
        assert_eq!(ids(&db, kept).await, 3, "{end:?}: the rows");
        exec(&db, &format!("INSERT INTO {kept} (parent, name) VALUES (2, 'd')")).await.expect("insert");
        assert_eq!(count(&db, &format!("SELECT id FROM {kept} WHERE id = 4")).await, 1, "{end:?}: the SERIAL went on from the rows");
        assert!(
            exec(&db, &format!("INSERT INTO {kept} (parent, name) VALUES (99, 'x')")).await.is_err(),
            "{end:?}: the self-reference is not enforced"
        );
        assert!(exec(&db, "INSERT INTO c (id, t_id) VALUES (2, 99)").await.is_err(), "{end:?}: the child's key is not enforced");
        exec(&db, "INSERT INTO c (id, t_id) VALUES (3, 2)").await.expect("a child row naming a parent that is there");
        assert!(block_leftovers(dir.path()).is_empty(), "{end:?}: left behind: {:?}", block_leftovers(dir.path()));
        shut(db, dir.path());
    }
}

/// `DROP TABLE t` then `CREATE TABLE t` in one block (2026-11-12): the old
/// table is set aside under a hidden name rather than dropped, so a
/// ROLLBACK, or a crash before COMMIT, brings it back with its rows, its
/// counter and the key a child holds on it; a COMMIT drops it, also when a
/// crash comes before the drop runs. The drop used to run at the CREATE,
/// and a rolled-back block lost the table.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_table_dropped_and_created_again_in_a_block_rolls_back_recovers_and_commits() {
    for end in [End::Rollback, End::Crash, End::Commit, End::CommitCrash] {
        let dir = tempfile::tempdir().expect("dir");
        let db = Arc::new(open(dir.path()).await);
        for sql in [
            "CREATE TABLE t (id SERIAL PRIMARY KEY, name TEXT, parent INT REFERENCES t(id))",
            "CREATE TABLE c (id INT PRIMARY KEY, t_id INT REFERENCES t(id))",
            "INSERT INTO t (name) VALUES ('a')",
            "INSERT INTO t (name, parent) VALUES ('b', 1)",
            "INSERT INTO t (name) VALUES ('c')",
            "INSERT INTO c (id, t_id) VALUES (1, 1)",
        ] {
            exec(&db, sql).await.expect(sql);
        }
        let root = db.transactions.begin();
        block_drop(&db, root, "t", "DROP TABLE t CASCADE").await;
        block_create_over(&db, root, "t", "CREATE TABLE t (id SERIAL PRIMARY KEY, other TEXT)").await;
        in_block(&db, root, "INSERT INTO t (other) VALUES ('x')").await.expect("insert into the new table");
        assert_eq!(columns(&db, "t"), vec!["id".to_string(), "other".to_string()], "{end:?}: the new table");
        in_block(&db, root, "INSERT INTO c (id, t_id) VALUES (7, 999)").await.expect("the dropped table's key went with it");
        // The set-aside table's own key names it, not the new table: a row
        // of its naming key 1 does not hold the new table's key 1.
        in_block(&db, root, "DELETE FROM t WHERE id = 1").await.expect("the new table's row is its own to delete");
        in_block(&db, root, "INSERT INTO t (other) VALUES ('y')").await.expect("insert");
        let db = end_block(db, root, end, dir.path()).await;
        match end {
            End::Commit | End::CommitCrash => {
                assert_eq!(columns(&db, "t"), vec!["id".to_string(), "other".to_string()], "{end:?}");
                assert_eq!(count(&db, "SELECT id FROM t WHERE id = 2").await, 1, "{end:?}: the new table's counter started at 1");
                assert_eq!(ids(&db, "t").await, 1, "{end:?}: the new table's rows");
                exec(&db, "INSERT INTO c (id, t_id) VALUES (2, 999)").await.expect("the key went with the dropped table");
            }
            _ => {
                assert_eq!(columns(&db, "t"), vec!["id".to_string(), "name".to_string(), "parent".to_string()], "{end:?}: the old table");
                assert_eq!(ids(&db, "t").await, 3, "{end:?}: its rows");
                exec(&db, "INSERT INTO t (name) VALUES ('d')").await.expect("insert");
                assert_eq!(count(&db, "SELECT id FROM t WHERE id = 4").await, 1, "{end:?}: its counter went on from its rows");
                assert!(exec(&db, "INSERT INTO t (name, parent) VALUES ('z', 99)").await.is_err(), "{end:?}: its own key is back");
                assert!(exec(&db, "INSERT INTO c (id, t_id) VALUES (2, 999)").await.is_err(), "{end:?}: the child's key is back");
            }
        }
        assert!(asides(&db, dir.path()).is_empty(), "{end:?}: set aside and left: {:?}", asides(&db, dir.path()));
        assert!(block_leftovers(dir.path()).is_empty(), "{end:?}: left behind: {:?}", block_leftovers(dir.path()));
        shut(db, dir.path());
    }
}

/// A block's deferred drop runs once its commit is durable (2026-11-12);
/// a crash between the two leaves an intent the next open finishes, for
/// the table the block dropped and never one created under its name since
/// (the intent names it by incarnation).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_committed_drop_a_crash_cut_short_is_finished_for_that_table_only() {
    let dir = tempfile::tempdir().expect("dir");
    let db = Arc::new(open(dir.path()).await);
    exec(&db, "CREATE TABLE gone (id INT PRIMARY KEY)").await.expect("create");
    exec(&db, "CREATE TABLE kept (id INT PRIMARY KEY)").await.expect("create");
    exec(&db, "INSERT INTO kept (id) VALUES (1)").await.expect("insert");
    let root = db.transactions.begin();
    block_drop(&db, root, "gone", "DROP TABLE gone").await;
    // An intent as a committed drop of an earlier "kept" leaves it: the
    // name, and an incarnation the table there now does not have.
    let stale = block_ddl::Undo::DropAtCommit {
        table: "kept".into(),
        sql: "DROP TABLE kept".into(),
        incarnation: db.incarnation_of("kept") ^ 1,
    };
    let path = block_ddl::intent_path(&dir.path().to_string_lossy(), root, 99);
    crate::core::database::rewrite::write_intent(&path, &block_ddl::encode(root, &stale)).expect("intent");
    let db = end_block(db, root, End::CommitCrash, dir.path()).await;
    assert!(db.relation("gone").is_none(), "the committed drop was not finished");
    assert!(db.relation("kept").is_some(), "a table that is not the one dropped was dropped");
    assert_eq!(ids(&db, "kept").await, 1);
    assert!(block_leftovers(dir.path()).is_empty(), "left behind: {:?}", block_leftovers(dir.path()));
    shut(db, dir.path());
}

/// A step leaving from the middle of a block's list (a partitioned table
/// rewritten into a new uid forgets its copies) must not hand its number on
/// to a live step's intent file: numbered by the list's length, the next
/// step took the number of the one after it, wrote its intent over that
/// one's, and a crash then left that one's change in place.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_new_step_never_takes_a_live_steps_number() {
    let dir = tempfile::tempdir().expect("dir");
    let db = Arc::new(open(dir.path()).await);
    exec(&db, "CREATE TABLE p (id INT NOT NULL, v TEXT) PARTITION BY HASH (id)").await.expect("create");
    exec(&db, "INSERT INTO p (id, v) VALUES (1, 'a'), (2, 'b')").await.expect("insert");
    exec(&db, "CREATE TABLE other (id INT PRIMARY KEY)").await.expect("create");
    exec(&db, "INSERT INTO other (id) VALUES (1)").await.expect("insert");
    let root = db.transactions.begin();
    block_ddl(
        &db,
        root,
        &[
            "CREATE INDEX p_v ON p (v)",
            "ALTER TABLE other ADD COLUMN extra INT",
            "ALTER TABLE p ADD COLUMN w INT",
            "CREATE TABLE z (id INT PRIMARY KEY)",
        ],
    )
    .await;
    let db = end_block(db, root, End::Crash, dir.path()).await;
    assert_eq!(columns(&db, "other"), vec!["id".to_string()], "the other table's ALTER survived the crash");
    assert!(db.relation("z").is_none(), "the created table survived the crash");
    assert_eq!(ids(&db, "other").await, 1);
    shut(db, dir.path());
}
