//! Database branches end to end, through pgwire's statement path:
//! `CREATE BRANCH`, `SHOW BRANCHES`, `RESET`/`PROMOTE`/`DROP BRANCH`,
//! `SHOW BRANCH DIFF` and `MERGE BRANCH` (docs/branching.md).

use super::native_session::{NativeSink, Said, read_messages};
use super::*;

fn init_globals() {
    let _ = crate::MAX_PERMITS_THREADS.set(4);
    let _ = crate::BATCH_SIZE.set(512);
}

/// A client session on one database of `catalog`.
struct Client {
    catalog: Arc<DatabaseCatalog>,
    name: String,
    state: SessionState,
}

impl Client {
    fn connect(catalog: &Arc<DatabaseCatalog>, name: &str) -> Client {
        let state = SessionState::new(
            catalog.clone(),
            name.to_string(),
            AuthConfig::default(),
            Principal {
                name: "tester".to_string(),
                superuser: true,
                authenticated: true,
            },
            ConnectionLimits::default(),
        );
        Client {
            catalog: catalog.clone(),
            name: name.to_string(),
            state,
        }
    }

    async fn try_exec(&mut self, sql: &str) -> Result<Said, (String, String)> {
        let database = self.catalog.get(&self.name).expect("connected database");
        self.state.message_us = crate::core::statement_clock::wall_us();
        self.state.error_in_batch = false;
        let mut sink = NativeSink::default();
        Box::pin(execute_recorded(
            &database,
            &mut self.state,
            &mut sink,
            sql,
            &[],
            false,
            None,
        ))
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
        self.state.statement_failed = false;
        let said = read_messages(&sink.out).expect("readable answer");
        match said.error.clone() {
            Some(e) => Err(e),
            None => Ok(said),
        }
    }

    async fn exec(&mut self, sql: &str) -> Said {
        self.try_exec(sql)
            .await
            .unwrap_or_else(|(code, message)| panic!("{sql}: {message} ({code})"))
    }

    async fn rows(&mut self, sql: &str) -> Vec<Vec<String>> {
        self.exec(sql)
            .await
            .rows
            .into_iter()
            .map(|r| {
                r.into_iter()
                    .map(|c| {
                        c.map(|b| String::from_utf8_lossy(&b).into_owned())
                            .unwrap_or_else(|| "NULL".into())
                    })
                    .collect()
            })
            .collect()
    }

    async fn scalar(&mut self, sql: &str) -> String {
        self.rows(sql).await.remove(0).remove(0)
    }
}

/// A catalogue on a fresh directory, and what to forget when it closes.
struct Fixture {
    _tmp: tempfile::TempDir,
    root: std::path::PathBuf,
    catalog: Arc<DatabaseCatalog>,
}

impl Fixture {
    async fn new(tag: &str) -> Fixture {
        init_globals();
        let tmp = tempfile::Builder::new()
            .prefix(&format!("rastdb_branch_{tag}_"))
            .tempdir()
            .unwrap();
        let root = tmp.path().to_path_buf();
        let catalog = DatabaseCatalog::open(Some(&root.to_string_lossy()), "postgres")
            .await
            .expect("catalogue");
        Fixture {
            _tmp: tmp,
            root,
            catalog,
        }
    }

    /// Closes the catalogue and opens it again, as a restart does.
    async fn reopen(&mut self) {
        self.forget();
        self.catalog = DatabaseCatalog::open(Some(&self.root.to_string_lossy()), "postgres")
            .await
            .expect("catalogue reopens");
    }

    fn forget(&self) {
        let mut dirs = vec![self.root.clone()];
        let mut walk = vec![self.root.clone()];
        while let Some(d) = walk.pop() {
            if let Ok(entries) = std::fs::read_dir(&d) {
                for e in entries.flatten() {
                    if e.file_type().map(|t| t.is_dir()).unwrap_or(false) {
                        dirs.push(e.path());
                        walk.push(e.path());
                    }
                }
            }
        }
        for d in dirs {
            let _ = crate::core::wal::remove_for(&d.to_string_lossy());
        }
    }

    fn client(&self, name: &str) -> Client {
        Client::connect(&self.catalog, name)
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        self.forget();
    }
}

async fn seeded(tag: &str) -> Fixture {
    let fx = Fixture::new(tag).await;
    let mut admin = fx.client("postgres");
    admin.exec("CREATE DATABASE prod").await;
    let mut prod = fx.client("prod");
    prod.exec("CREATE TABLE items (id INT PRIMARY KEY, name TEXT, qty INT)")
        .await;
    prod.exec("INSERT INTO items (id, name, qty) VALUES (1, 'apple', 10), (2, 'pear', 20), (3, 'plum', 30)")
        .await;
    fx
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_branch_holds_the_parent_and_diverges_from_it() {
    let fx = seeded("diverge").await;
    let mut admin = fx.client("postgres");
    admin.exec("CREATE BRANCH dev FROM prod").await;

    let mut dev = fx.client("dev");
    assert_eq!(dev.scalar("SELECT COUNT(*) FROM items").await, "3");
    dev.exec("INSERT INTO items (id, name, qty) VALUES (4, 'fig', 40)")
        .await;
    dev.exec("UPDATE items SET qty = 11 WHERE id = 1").await;

    let mut prod = fx.client("prod");
    prod.exec("DELETE FROM items WHERE id = 3").await;
    assert_eq!(prod.scalar("SELECT COUNT(*) FROM items").await, "2");
    assert_eq!(
        prod.scalar("SELECT qty FROM items WHERE id = 1").await,
        "10"
    );
    assert_eq!(dev.scalar("SELECT COUNT(*) FROM items").await, "4");

    let branches = admin.rows("SHOW BRANCHES").await;
    assert_eq!(branches.len(), 1, "{branches:?}");
    assert_eq!(branches[0][0], "dev");
    assert_eq!(branches[0][1], "prod");
    assert_eq!(branches[0][2], "t");
    assert_eq!(branches[0][3], "now");
    assert!(
        branches[0][9].parse::<u64>().unwrap() > 0,
        "no base size: {branches:?}"
    );

    // The only proof a copy reads its own files: the source gone.
    admin.exec("DROP DATABASE prod").await;
    assert!(!fx.root.join("prod").exists());
    let mut dev = fx.client("dev");
    assert_eq!(dev.scalar("SELECT COUNT(*) FROM items").await, "4");
    assert_eq!(dev.scalar("SELECT qty FROM items WHERE id = 1").await, "11");
    dev.exec("INSERT INTO items (id, name, qty) VALUES (5, 'kiwi', 50)")
        .await;
    assert_eq!(dev.scalar("SELECT COUNT(*) FROM items").await, "5");
    let listed = admin.rows("SHOW BRANCHES").await;
    assert_eq!(listed[0][2], "f", "the parent is gone: {listed:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_branch_of_the_default_database_leaves_the_others_out() {
    let fx = seeded("default").await;
    let mut admin = fx.client("postgres");
    admin
        .exec("CREATE TABLE notes (id INT PRIMARY KEY, body TEXT)")
        .await;
    admin
        .exec("INSERT INTO notes (id, body) VALUES (1, 'root')")
        .await;
    admin.exec("CREATE BRANCH scratch").await;
    let dir = fx.root.join("scratch");
    assert!(
        !dir.join("prod").exists(),
        "another database was copied into the branch"
    );
    let mut scratch = fx.client("scratch");
    assert_eq!(
        scratch.scalar("SELECT body FROM notes WHERE id = 1").await,
        "root"
    );
    assert!(
        scratch.try_exec("SELECT * FROM items").await.is_err(),
        "prod's table leaked into the branch"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_branch_taken_under_writes_holds_whole_transactions() {
    let fx = seeded("writes").await;
    let mut prod = fx.client("prod");
    prod.exec("CREATE TABLE pairs (id INT PRIMARY KEY, side TEXT, pair INT)")
        .await;

    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    // Two-row transactions: a copy that caught one row of one is wrong.
    let (catalog, flag) = (fx.catalog.clone(), stop.clone());
    let writer = tokio::spawn(async move {
        let mut c = Client::connect(&catalog, "prod");
        let mut n = 0;
        while !flag.load(std::sync::atomic::Ordering::Relaxed) && n < 400 {
            c.exec("BEGIN").await;
            c.exec(&format!(
                "INSERT INTO pairs (id, side, pair) VALUES ({}, 'a', {n})",
                n * 2
            ))
            .await;
            c.exec(&format!(
                "INSERT INTO pairs (id, side, pair) VALUES ({}, 'b', {n})",
                n * 2 + 1
            ))
            .await;
            c.exec("COMMIT").await;
            n += 1;
        }
        n
    });
    // Tables made while the copy runs: one the copy's listing missed must
    // still come across whole, from the log.
    let (catalog, flag) = (fx.catalog.clone(), stop.clone());
    let creator = tokio::spawn(async move {
        let mut c = Client::connect(&catalog, "prod");
        let mut n = 0;
        while !flag.load(std::sync::atomic::Ordering::Relaxed) && n < 60 {
            c.exec(&format!(
                "CREATE TABLE made_{n} (id INT PRIMARY KEY, v TEXT)"
            ))
            .await;
            c.exec(&format!("INSERT INTO made_{n} (id, v) VALUES (1, 'x')"))
                .await;
            n += 1;
        }
        n
    });
    tokio::time::sleep(std::time::Duration::from_millis(150)).await;
    let mut admin = fx.client("postgres");
    admin
        .exec("CREATE BRANCH snap FROM prod WITHOUT BASE")
        .await;
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    let written = writer.await.unwrap();
    let made = creator.await.unwrap();

    let mut snap = fx.client("snap");
    let rows = snap.rows("SELECT pair, side FROM pairs").await;
    let mut by_pair: std::collections::BTreeMap<String, Vec<String>> = Default::default();
    for r in &rows {
        by_pair.entry(r[0].clone()).or_default().push(r[1].clone());
    }
    let broken: Vec<_> = by_pair
        .iter()
        .filter(|(_, sides)| sides.len() != 2)
        .collect();
    assert!(
        broken.is_empty(),
        "transactions came across half-applied: {broken:?}"
    );
    assert!(!by_pair.is_empty(), "the branch was taken before any write");
    assert!(by_pair.len() <= written as usize);

    let branch_db = fx.catalog.get("snap").unwrap();
    let tables: Vec<String> = branch_db
        .relation_names()
        .into_iter()
        .filter(|t| t.starts_with("made_"))
        .collect();
    assert!(tables.len() <= made as usize);
    for t in tables {
        match snap.try_exec(&format!("SELECT COUNT(*) FROM {t}")).await {
            // Its row is there, or its CREATE came across without its INSERT.
            Ok(said) => {
                let count = String::from_utf8_lossy(said.rows[0][0].as_ref().unwrap()).into_owned();
                assert!(count == "0" || count == "1", "{t}: {count}");
            }
            Err(e) => panic!("{t} is in the branch's catalogue but cannot be read: {e:?}"),
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reset_discards_the_branch_and_promote_swaps_it_in() {
    let fx = seeded("reset").await;
    let mut admin = fx.client("postgres");
    admin.exec("CREATE BRANCH dev FROM prod").await;
    let mut dev = fx.client("dev");
    dev.exec("DELETE FROM items").await;
    let mut prod = fx.client("prod");
    prod.exec("INSERT INTO items (id, name, qty) VALUES (9, 'new', 90)")
        .await;

    admin.exec("RESET BRANCH dev").await;
    let mut dev = fx.client("dev");
    assert_eq!(
        dev.scalar("SELECT COUNT(*) FROM items").await,
        "4",
        "reset did not copy the parent again"
    );

    dev.exec("UPDATE items SET name = 'promoted' WHERE id = 1")
        .await;
    admin.exec("PROMOTE BRANCH dev").await;
    let mut prod = fx.client("prod");
    assert_eq!(
        prod.scalar("SELECT name FROM items WHERE id = 1").await,
        "promoted"
    );
    let mut old = fx.client("dev");
    assert_eq!(
        old.scalar("SELECT name FROM items WHERE id = 1").await,
        "apple"
    );
    let listed = admin.rows("SHOW BRANCHES").await;
    assert_eq!(listed.len(), 1, "{listed:?}");
    assert_eq!(
        (listed[0][0].as_str(), listed[0][1].as_str()),
        ("dev", "prod")
    );

    // The old parent, now a branch, goes with DROP BRANCH; a database that
    // is not a branch does not.
    let refused = admin
        .try_exec("DROP BRANCH prod")
        .await
        .expect_err("dropped a non-branch");
    assert_eq!(refused.0, "42704");
    admin.exec("DROP BRANCH dev").await;
    assert!(fx.catalog.get("dev").is_none());
    assert!(fx.catalog.get("prod").is_some());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn create_database_template_is_a_copy() {
    let fx = seeded("template").await;
    let mut admin = fx.client("postgres");
    admin
        .exec("CREATE DATABASE copy1 WITH TEMPLATE = prod")
        .await;
    admin
        .exec("CREATE DATABASE empty1 TEMPLATE template0")
        .await;
    let mut copy1 = fx.client("copy1");
    assert_eq!(copy1.scalar("SELECT COUNT(*) FROM items").await, "3");
    let mut empty1 = fx.client("empty1");
    assert!(empty1.try_exec("SELECT * FROM items").await.is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_merge_applies_what_the_branch_did() {
    let fx = seeded("merge").await;
    let mut admin = fx.client("postgres");
    admin.exec("CREATE BRANCH dev FROM prod").await;

    let mut dev = fx.client("dev");
    dev.exec("ALTER TABLE items ADD COLUMN note TEXT").await;
    dev.exec("UPDATE items SET qty = 21, note = 'restocked' WHERE id = 2")
        .await;
    // Changed only in the column the branch added.
    dev.exec("UPDATE items SET note = 'first' WHERE id = 1")
        .await;
    dev.exec("DELETE FROM items WHERE id = 3").await;
    dev.exec("INSERT INTO items (id, name, qty, note) VALUES (4, 'fig', 40, 'new')")
        .await;
    dev.exec("CREATE TABLE tags (id INT PRIMARY KEY, label TEXT)")
        .await;
    dev.exec("INSERT INTO tags (id, label) VALUES (1, 'fresh')")
        .await;

    // The parent's own change elsewhere stays.
    let mut prod = fx.client("prod");
    prod.exec("INSERT INTO items (id, name, qty) VALUES (7, 'lime', 70)")
        .await;

    let diff = admin.rows("SHOW BRANCH DIFF dev").await;
    let items_data = diff
        .iter()
        .find(|r| r[0] == "items" && r[1] == "data")
        .expect("items data row");
    assert_eq!(&items_data[2..6], &["1", "2", "1", "0"], "{diff:?}");
    assert!(
        diff.iter().any(|r| r[0] == "tags" && r[1] == "schema"),
        "{diff:?}"
    );
    assert!(
        diff.iter()
            .any(|r| r[0] == "items" && r[1] == "schema" && r[6].contains("note")),
        "{diff:?}"
    );

    admin.exec("MERGE BRANCH dev").await;
    let mut prod = fx.client("prod");
    assert_eq!(
        prod.rows("SELECT id, qty, note FROM items ORDER BY id")
            .await,
        vec![
            vec!["1".to_string(), "10".into(), "first".into()],
            vec!["2".into(), "21".into(), "restocked".into()],
            vec!["4".into(), "40".into(), "new".into()],
            vec!["7".into(), "70".into(), "NULL".into()],
        ]
    );
    assert_eq!(
        prod.scalar("SELECT label FROM tags WHERE id = 1").await,
        "fresh"
    );
    // The table the merge created is the merging role's, not an internal
    // session's that no role is.
    assert_eq!(
        prod.rows("SELECT grantee FROM __rdb_acl WHERE relation = 'tags' AND privilege = 'OWNER'")
            .await,
        vec![vec!["tester".to_string()]]
    );

    // Merged once: nothing is offered again.
    let again = admin.rows("SHOW BRANCH DIFF dev").await;
    assert!(
        again.is_empty(),
        "a merged change was offered again: {again:?}"
    );
    let listed = admin.rows("SHOW BRANCHES").await;
    assert!(
        !listed[0][7].is_empty(),
        "merged_at not recorded: {listed:?}"
    );

    // And later changes merge on their own.
    let mut dev = fx.client("dev");
    dev.exec("UPDATE items SET qty = 41 WHERE id = 4").await;
    admin.exec("MERGE BRANCH dev").await;
    assert_eq!(
        prod.scalar("SELECT qty FROM items WHERE id = 4").await,
        "41"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_conflict_fails_the_merge_whole_unless_a_side_is_chosen() {
    let fx = seeded("conflict").await;
    let mut admin = fx.client("postgres");
    admin.exec("CREATE BRANCH dev FROM prod").await;
    let mut dev = fx.client("dev");
    dev.exec("UPDATE items SET qty = 100 WHERE id = 1").await;
    dev.exec("INSERT INTO items (id, name, qty) VALUES (5, 'date', 50)")
        .await;
    let mut prod = fx.client("prod");
    prod.exec("UPDATE items SET qty = 200 WHERE id = 1").await;

    let (code, message) = admin
        .try_exec("MERGE BRANCH dev")
        .await
        .expect_err("a conflict merged");
    assert_eq!(code, "40001", "{message}");
    assert!(message.contains("id=1"), "{message}");
    // Nothing applied, not even the row that did not conflict.
    assert_eq!(prod.scalar("SELECT COUNT(*) FROM items").await, "3");
    assert_eq!(
        prod.scalar("SELECT qty FROM items WHERE id = 1").await,
        "200"
    );

    admin.exec("MERGE BRANCH dev ON CONFLICT PARENT WINS").await;
    assert_eq!(
        prod.scalar("SELECT qty FROM items WHERE id = 1").await,
        "200"
    );
    assert_eq!(prod.scalar("SELECT COUNT(*) FROM items").await, "4");

    let mut dev = fx.client("dev");
    dev.exec("UPDATE items SET qty = 101 WHERE id = 1").await;
    prod.exec("UPDATE items SET qty = 201 WHERE id = 1").await;
    admin.exec("MERGE BRANCH dev ON CONFLICT BRANCH WINS").await;
    assert_eq!(
        prod.scalar("SELECT qty FROM items WHERE id = 1").await,
        "101"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_destructive_merge_needs_allow_drop_and_rolled_back_ddl_is_not_recorded() {
    let fx = seeded("drop").await;
    let mut admin = fx.client("postgres");
    admin.exec("CREATE BRANCH dev FROM prod").await;
    let mut dev = fx.client("dev");
    // Rolled back: never recorded, never merged.
    dev.exec("BEGIN").await;
    dev.exec("CREATE TABLE ghost (id INT PRIMARY KEY)").await;
    dev.exec("ROLLBACK").await;
    dev.exec("ALTER TABLE items DROP COLUMN qty").await;

    let (code, message) = admin
        .try_exec("MERGE BRANCH dev")
        .await
        .expect_err("dropped data without ALLOW DROP");
    assert_eq!(code, "2BP01", "{message}");
    admin.exec("MERGE BRANCH dev ALLOW DROP").await;
    let mut prod = fx.client("prod");
    assert!(
        prod.try_exec("SELECT qty FROM items").await.is_err(),
        "the column was not dropped"
    );
    assert!(
        prod.try_exec("SELECT * FROM ghost").await.is_err(),
        "rolled-back DDL was merged"
    );
    assert_eq!(prod.scalar("SELECT COUNT(*) FROM items").await, "3");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn branches_survive_a_restart_and_leftovers_are_swept() {
    let mut fx = seeded("restart").await;
    let mut admin = fx.client("postgres");
    admin.exec("CREATE BRANCH dev FROM prod").await;
    let mut dev = fx.client("dev");
    dev.exec("INSERT INTO items (id, name, qty) VALUES (8, 'yuzu', 80)")
        .await;
    // What a crash in the middle of a branch leaves.
    std::fs::create_dir_all(fx.root.join(".branching-half")).unwrap();
    std::fs::write(fx.root.join(".branching-half").join("x.db"), b"junk").unwrap();

    fx.reopen().await;
    assert!(
        !fx.root.join(".branching-half").exists(),
        "a half-built branch was left"
    );
    let mut admin = fx.client("postgres");
    let listed = admin.rows("SHOW BRANCHES").await;
    assert_eq!(listed.len(), 1, "{listed:?}");
    let mut dev = fx.client("dev");
    assert_eq!(dev.scalar("SELECT COUNT(*) FROM items").await, "4");
    // Its base came through the restart too: the diff still reads it.
    let diff = admin.rows("SHOW BRANCH DIFF dev").await;
    assert_eq!(diff.len(), 1, "{diff:?}");
    assert_eq!(&diff[0][2..5], &["1", "0", "0"]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn refusals_name_the_problem() {
    let fx = seeded("refuse").await;
    let mut admin = fx.client("postgres");
    let e = admin
        .try_exec("CREATE BRANCH dev FROM nowhere")
        .await
        .unwrap_err();
    assert_eq!(e.0, "3D000", "{e:?}");
    admin.exec("CREATE BRANCH dev FROM prod WITHOUT BASE").await;
    let e = admin
        .try_exec("CREATE BRANCH dev FROM prod")
        .await
        .unwrap_err();
    assert_eq!(e.0, "42P04", "{e:?}");
    let e = admin.try_exec("MERGE BRANCH dev").await.unwrap_err();
    assert!(e.1.contains("no merge base"), "{e:?}");
    let e = admin.try_exec("MERGE BRANCH prod").await.unwrap_err();
    assert_eq!(e.0, "42704", "{e:?}");
    let mut dev = fx.client("dev");
    let e = dev.try_exec("RESET BRANCH dev").await.unwrap_err();
    assert!(e.1.contains("connected"), "{e:?}");
    admin.exec("BEGIN").await;
    let e = admin
        .try_exec("CREATE BRANCH other FROM prod")
        .await
        .unwrap_err();
    assert_eq!(e.0, "25001", "{e:?}");
    admin.exec("ROLLBACK").await;
    let e = admin
        .try_exec("CREATE BRANCH later FROM prod AS OF LSN 1")
        .await
        .unwrap_err();
    assert!(e.1.contains("S3 backup"), "{e:?}");
    // A diff reads other databases' rows: not for an ordinary account.
    let mut plain = fx.client("dev");
    plain.state.principal = Principal {
        name: "plain".to_string(),
        superuser: false,
        authenticated: true,
    };
    for sql in [
        "SHOW BRANCH DIFF dev",
        "MERGE BRANCH dev",
        "CREATE BRANCH x FROM prod",
        "DROP BRANCH dev",
    ] {
        let e = plain.try_exec(sql).await.unwrap_err();
        assert_eq!(e.0, "42501", "{sql}: {e:?}");
    }
    assert!(plain.try_exec("SHOW BRANCHES").await.is_ok());
    assert!(!fx.root.join(".branching-later").exists());
    assert!(fx.catalog.get("later").is_none());
}

#[cfg(feature = "s3_backup")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_branch_at_a_past_point_is_restored_from_the_backup() {
    use crate::core::backup::s3::{Credentials, Endpoint};
    let fx = seeded("pitr").await;
    let root = fx.root.to_string_lossy().into_owned();
    let config = crate::core::backup::BackupConfig {
        bucket: "rdbtest".to_string(),
        prefix: format!("branch-pitr/{:016x}", fastrand::u64(..)),
        region: "us-east-1".to_string(),
        endpoint: Endpoint::parse(&crate::core::backup::fake_s3::shared().endpoint()).unwrap(),
        credentials: Credentials {
            access_key: "fake".to_string(),
            secret_key: "fake".to_string(),
            session_token: None,
        },
        path_style: true,
        snapshot_level: 3,
        segment_level: 3,
        compression_threads: 1,
        long_range: false,
        part_size: 5 * 1024 * 1024,
        upload_concurrency: 2,
        max_attempts: 3,
        snapshot_interval_secs: 0,
        wal_flush_secs: 0,
        wal_bundle_bytes: 0,
        rollover_interval_secs: 0,
        retain_snapshots: 0,
        staging_max_bytes: 0,
        incremental_min_bytes: 0,
    };
    let service = crate::core::backup::BackupService::connect(config.clone(), &root)
        .await
        .expect("backup service");
    let report = service.snapshot_now().await.expect("snapshot");
    crate::core::database::catalog::branch::TEST_BACKUPS
        .lock()
        .insert(crate::core::wal::dir_key(&root), config);

    let mut prod = fx.client("prod");
    prod.exec("INSERT INTO items (id, name, qty) VALUES (4, 'fig', 40)")
        .await;
    prod.exec("UPDATE items SET qty = 0").await;

    let mut admin = fx.client("postgres");
    admin
        .exec(&format!(
            "CREATE BRANCH then FROM prod AS OF SNAPSHOT '{}'",
            report.id
        ))
        .await;
    let listed = admin.rows("SHOW BRANCHES").await;
    assert_eq!(listed[0][5], "restore", "{listed:?}");
    assert!(listed[0][3].starts_with("snapshot "), "{listed:?}");
    // The past, not the present, and its own files: the source dropped first.
    admin.exec("DROP DATABASE prod").await;
    let mut then = fx.client("then");
    assert_eq!(then.scalar("SELECT COUNT(*) FROM items").await, "3");
    assert_eq!(
        then.scalar("SELECT qty FROM items WHERE id = 2").await,
        "20"
    );
    assert!(!fx.root.read_dir().unwrap().flatten().any(|e| {
        e.file_name()
            .to_string_lossy()
            .starts_with(".branch-restore-")
    }));
    crate::core::database::catalog::branch::TEST_BACKUPS
        .lock()
        .remove(&crate::core::wal::dir_key(&root));
    drop(service);
}

/// Open descriptors of this process under `dir`.
fn open_under(dir: &std::path::Path) -> Vec<String> {
    let dir = std::fs::canonicalize(dir).unwrap_or_else(|_| dir.to_path_buf());
    let mut out = Vec::new();
    if let Ok(entries) = std::fs::read_dir("/proc/self/fd") {
        for e in entries.flatten() {
            if let Ok(target) = std::fs::read_link(e.path())
                && target.starts_with(&dir)
            {
                out.push(target.to_string_lossy().into_owned());
            }
        }
    }
    out.sort();
    out
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_dropped_database_lets_go_of_everything() {
    let fx = seeded("letgo").await;
    let mut prod = fx.client("prod");
    prod.exec("CREATE INDEX items_qty ON items (qty)").await;
    prod.exec("CREATE TABLE parts (id INT PRIMARY KEY, item INT REFERENCES items(id)) PARTITION BY HASH (id) PARTITIONS 2").await;
    prod.exec("INSERT INTO parts (id, item) VALUES (1, 1), (2, 2)").await;
    assert_eq!(prod.scalar("SELECT COUNT(*) FROM parts").await, "2");
    drop(prod);
    let dir = fx.root.join("prod");
    let held = fx.catalog.get("prod").unwrap();
    let mut admin = fx.client("postgres");
    admin.exec("DROP DATABASE prod").await;
    let mut others = usize::MAX;
    for _ in 0..100 {
        others = Arc::strong_count(&held) - 1;
        if others == 0 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    drop(held);
    let mut fds = Vec::new();
    for _ in 0..100 {
        fds = open_under(&dir);
        if fds.is_empty() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    let left = (others != 0, fds);
    assert!(!left.0 && left.1.is_empty(), "the dropped database is still held: {left:?}");

    // A branch's RESET and MERGE retire copies of it too: none is held
    // open once they return.
    let fx = seeded("letgo2").await;
    let mut admin = fx.client("postgres");
    admin.exec("CREATE BRANCH dev FROM prod").await;
    let mut dev = fx.client("dev");
    dev.exec("INSERT INTO items (id, name, qty) VALUES (5, 'x', 1)").await;
    admin.exec("MERGE BRANCH dev").await;
    admin.exec("RESET BRANCH dev").await;
    admin.exec("MERGE BRANCH dev").await;
    drop(dev);
    let deleted = |root: &std::path::Path| -> Vec<String> {
        open_under(root).into_iter().filter(|p| p.ends_with("(deleted)")).collect()
    };
    let mut held = deleted(&fx.root);
    for _ in 0..50 {
        if held.is_empty() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        held = deleted(&fx.root);
    }
    assert!(held.is_empty(), "retired copies still open: {held:?}");
    assert!(!fx.root.join(".retiring-dev").exists());
}

/// `SHOW BRANCHES` as `(branch, parent, depth, path, children, via)`.
async fn tree(admin: &mut Client) -> Vec<(String, String, String, String, String, String)> {
    admin
        .rows("SHOW BRANCHES")
        .await
        .into_iter()
        .map(|r| {
            (
                r[0].clone(),
                r[1].clone(),
                r[11].clone(),
                r[12].clone(),
                r[13].clone(),
                r[14].clone(),
            )
        })
        .collect()
}

fn row(cells: &[&str]) -> Vec<String> {
    cells.iter().map(|c| c.to_string()).collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_branch_of_a_branch_merges_up_the_chain_and_outlives_its_middle() {
    let fx = seeded("nested").await;
    let mut admin = fx.client("postgres");
    admin.exec("CREATE BRANCH dev FROM prod").await;
    let mut dev = fx.client("dev");
    dev.exec("ALTER TABLE items ADD COLUMN note TEXT").await;
    dev.exec("UPDATE items SET note = 'd' WHERE id = 1").await;
    dev.exec("INSERT INTO items (id, name, qty, note) VALUES (4, 'fig', 40, NULL)")
        .await;

    admin.exec("CREATE BRANCH feature FROM dev").await;
    let mut feature = fx.client("feature");
    // The copy carries the middle branch's data, not its record of DDL.
    assert_eq!(feature.scalar("SELECT note FROM items WHERE id = 1").await, "d");
    feature
        .exec("CREATE TABLE tags (id INT PRIMARY KEY, label TEXT)")
        .await;
    feature.exec("INSERT INTO tags (id, label) VALUES (1, 'x')").await;
    feature.exec("UPDATE items SET qty = 99 WHERE id = 2").await;
    feature
        .exec("INSERT INTO items (id, name, qty, note) VALUES (5, 'kiwi', 50, 'f')")
        .await;

    let listed = tree(&mut admin).await;
    assert_eq!(
        listed,
        vec![
            ("dev".into(), "prod".into(), "1".into(), "prod/dev".into(), "1".into(), String::new()),
            ("feature".into(), "dev".into(), "2".into(), "prod/dev/feature".into(), "0".into(), String::new()),
        ]
    );

    // Up one level: into dev, which records the replayed DDL as its own.
    admin.exec("MERGE BRANCH feature").await;
    let mut dev = fx.client("dev");
    assert_eq!(dev.scalar("SELECT qty FROM items WHERE id = 2").await, "99");
    assert_eq!(dev.scalar("SELECT label FROM tags WHERE id = 1").await, "x");
    // And up the next: prod takes dev's own changes and feature's, schema
    // included.
    admin.exec("MERGE BRANCH dev").await;
    let mut prod = fx.client("prod");
    assert_eq!(
        prod.rows("SELECT id, qty, note FROM items ORDER BY id").await,
        vec![
            row(&["1", "10", "d"]),
            row(&["2", "99", "NULL"]),
            row(&["3", "30", "NULL"]),
            row(&["4", "40", "NULL"]),
            row(&["5", "50", "f"]),
        ]
    );
    assert_eq!(prod.scalar("SELECT label FROM tags WHERE id = 1").await, "x");

    // The middle goes: its branch moves to prod, keeping the base it was
    // made with — including a column only dev had.
    let mut dev = fx.client("dev");
    dev.exec("ALTER TABLE items ADD COLUMN extra INT").await;
    admin.exec("CREATE BRANCH feature2 FROM dev").await;
    let mut feature2 = fx.client("feature2");
    feature2.exec("UPDATE items SET qty = 33 WHERE id = 3").await;
    feature2.exec("UPDATE items SET extra = 7 WHERE id = 1").await;
    feature2
        .exec("INSERT INTO items (id, name, qty) VALUES (6, 'lime', 60)")
        .await;
    drop((dev, feature, feature2));
    let said = admin.exec("DROP BRANCH dev").await;
    assert!(
        said.notices.iter().any(|n| n.contains("\"feature\", \"feature2\" now branch from \"prod\"")),
        "{:?}",
        said.notices
    );
    let listed = tree(&mut admin).await;
    assert_eq!(
        listed,
        vec![
            ("feature".into(), "prod".into(), "1".into(), "prod/feature".into(), "0".into(), "dev".into()),
            ("feature2".into(), "prod".into(), "1".into(), "prod/feature2".into(), "0".into(), "dev".into()),
        ]
    );
    let diff = admin.rows("SHOW BRANCH DIFF feature2").await;
    let items = diff
        .iter()
        .find(|r| r[0] == "items" && r[1] == "data")
        .unwrap_or_else(|| panic!("{diff:?}"));
    // Only feature2's own changes: dev's column is not prod's to take.
    assert_eq!(&items[2..6], &["1", "1", "0", "0"], "{diff:?}");
    assert!(items[6].contains("extra"), "{diff:?}");
    admin.exec("MERGE BRANCH feature2").await;
    assert_eq!(
        prod.rows("SELECT id, qty FROM items WHERE id IN (3, 6) ORDER BY id").await,
        vec![row(&["3", "33"]), row(&["6", "60"])]
    );
    assert!(prod.try_exec("SELECT extra FROM items").await.is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_promoted_middle_takes_its_branches_with_it() {
    let fx = seeded("promote_mid").await;
    let mut admin = fx.client("postgres");
    admin.exec("CREATE BRANCH dev FROM prod").await;
    admin.exec("CREATE BRANCH sibling FROM prod").await;
    let mut dev = fx.client("dev");
    dev.exec("UPDATE items SET name = 'dev' WHERE id = 1").await;
    admin.exec("CREATE BRANCH feature FROM dev").await;
    let mut feature = fx.client("feature");
    feature.exec("UPDATE items SET qty = 77 WHERE id = 1").await;

    admin.exec("PROMOTE BRANCH dev").await;
    let listed = tree(&mut admin).await;
    let parent_of = |b: &str| {
        listed
            .iter()
            .find(|r| r.0 == b)
            .map(|r| r.1.clone())
            .unwrap_or_else(|| panic!("{b} missing: {listed:?}"))
    };
    // dev's data is prod now, and its branch follows it; prod's other
    // branch stays with the name; the old prod is a branch of the new.
    assert_eq!(parent_of("feature"), "prod");
    assert_eq!(parent_of("sibling"), "prod");
    assert_eq!(parent_of("dev"), "prod");
    admin.exec("MERGE BRANCH feature").await;
    let mut prod = fx.client("prod");
    assert_eq!(
        prod.rows("SELECT name, qty FROM items WHERE id = 1").await,
        vec![row(&["dev", "77"])]
    );
}

#[cfg(all(feature = "storage_pools", feature = "kv_storage"))]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_table_on_a_store_comes_over_as_the_branchs_own() {
    let fx = seeded("kv").await;
    let kv = fx.root.join("kvstore");
    let mut prod = fx.client("prod");
    prod.exec(&format!(
        "STORAGE ADD STORE scratch DIRECTORY WITH (PATH = '{}', PREFIX = 'scratch', CHUNK_SIZE = '16KiB', PAGE_SIZE = '4KiB')",
        kv.display()
    ))
    .await;
    prod.exec("STORAGE PLACE TABLE events AT 'kv:scratch'").await;
    prod.exec("CREATE TABLE events (id INT PRIMARY KEY, what TEXT)").await;
    for i in 0..50 {
        prod.exec(&format!("INSERT INTO events (id, what) VALUES ({i}, 'e{i}')"))
            .await;
    }
    prod.exec("UPDATE events SET what = 'changed' WHERE id = 7").await;

    let mut admin = fx.client("postgres");
    let said = admin.exec("CREATE BRANCH dev FROM prod").await;
    assert!(
        said.notices.iter().any(|n| n.contains("events") && n.contains("key/value")),
        "{:?}",
        said.notices
    );
    let dev_dir = fx.root.join("dev");
    assert!(!dev_dir.join("db_kv.cfg").exists(), "the branch shares the parent's stores");
    let mut dev = fx.client("dev");
    assert_eq!(dev.scalar("SELECT COUNT(*) FROM events").await, "50");
    assert_eq!(dev.scalar("SELECT what FROM events WHERE id = 7").await, "changed");

    // Each side's writes are its own.
    dev.exec("DELETE FROM events WHERE id < 10").await;
    dev.exec("INSERT INTO events (id, what) VALUES (100, 'dev')").await;
    prod.exec("INSERT INTO events (id, what) VALUES (200, 'prod')").await;
    assert_eq!(dev.scalar("SELECT COUNT(*) FROM events").await, "41");
    assert_eq!(prod.scalar("SELECT COUNT(*) FROM events").await, "51");

    // And a restart reads both back the same.
    drop((dev, prod, admin));
    let mut fx = fx;
    fx.reopen().await;
    let mut dev = fx.client("dev");
    let mut prod = fx.client("prod");
    assert_eq!(dev.scalar("SELECT COUNT(*) FROM events").await, "41");
    assert_eq!(prod.scalar("SELECT COUNT(*) FROM events").await, "51");

    // The branch merges back onto the store's table.
    let mut admin = fx.client("postgres");
    admin.exec("MERGE BRANCH dev").await;
    assert_eq!(prod.scalar("SELECT COUNT(*) FROM events").await, "42");
    assert_eq!(prod.scalar("SELECT what FROM events WHERE id = 100").await, "dev");

    // Partitions on a store are refused, by name of the store.
    prod.exec("STORAGE PLACE TABLE logs AT 'kv:scratch'").await;
    prod.exec("CREATE TABLE logs (id INT PRIMARY KEY, n INT) PARTITION BY HASH (id) PARTITIONS 2")
        .await;
    prod.exec("INSERT INTO logs (id, n) VALUES (1, 1), (2, 2)").await;
    let refused = admin
        .try_exec("CREATE BRANCH other FROM prod")
        .await
        .expect_err("branched partitions on a store");
    assert!(refused.1.contains("'scratch'"), "{refused:?}");
    assert!(fx.catalog.get("other").is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_merge_pages_through_a_table_and_a_late_conflict_undoes_it_all() {
    // Pages are three rows in tests: thirty rows on a composite key take
    // a dozen rounds, with the sides' changes in different pages.
    let fx = Fixture::new("paged").await;
    let mut admin = fx.client("postgres");
    admin.exec("CREATE DATABASE prod").await;
    let mut prod = fx.client("prod");
    prod.exec("CREATE TABLE grid (a INT, b TEXT, v INT, w TEXT, PRIMARY KEY (a, b))")
        .await;
    let values: Vec<String> = (1..=30)
        .map(|i| format!("({}, '{}', {i}, 'w{i}')", i % 10, ["x", "y", "z"][i / 10 % 3]))
        .collect();
    prod.exec(&format!("INSERT INTO grid (a, b, v, w) VALUES {}", values.join(", ")))
        .await;
    let before = prod.rows("SELECT a, b, v, w FROM grid ORDER BY a, b").await;
    admin.exec("CREATE BRANCH dev FROM prod").await;

    let mut dev = fx.client("dev");
    dev.exec("DELETE FROM grid WHERE a = 1").await; // early pages
    dev.exec("UPDATE grid SET v = v + 100 WHERE a = 5").await; // middle
    dev.exec("INSERT INTO grid (a, b, v, w) VALUES (9, 'q', 900, 'new'), (0, 'a', 0, 'first')")
        .await;
    prod.exec("UPDATE grid SET w = 'parent' WHERE a = 7").await;
    prod.exec("INSERT INTO grid (a, b, v, w) VALUES (8, 'p', 800, 'p')").await;
    // Denser on the parent than on either other side: its pages end the
    // rounds' ranges there.
    prod.exec("INSERT INTO grid (a, b, v, w) VALUES (3, 'a', 1, 'p'), (3, 'b', 2, 'p'), (3, 'c', 3, 'p'), (3, 'd', 4, 'p')")
        .await;

    // A conflict in the last page fails the whole merge: the early pages'
    // deletes are undone with it.
    dev.exec("UPDATE grid SET w = 'branch' WHERE a = 9 AND b = 'z'").await;
    prod.exec("UPDATE grid SET w = 'theirs' WHERE a = 9 AND b = 'z'").await;
    let parent_before = prod.rows("SELECT a, b, v, w FROM grid ORDER BY a, b").await;
    let refused = admin
        .try_exec("MERGE BRANCH dev")
        .await
        .expect_err("a conflict merged");
    assert_eq!(refused.0, "40001", "{refused:?}");
    assert!(refused.1.contains("a=9, b=z"), "{refused:?}");
    assert_eq!(
        prod.rows("SELECT a, b, v, w FROM grid ORDER BY a, b").await,
        parent_before,
        "a failed merge left writes behind"
    );

    let diff = admin.rows("SHOW BRANCH DIFF dev").await;
    let grid = diff.iter().find(|r| r[0] == "grid").unwrap_or_else(|| panic!("{diff:?}"));
    assert_eq!(&grid[2..6], &["2", "3", "3", "1"], "{diff:?}");

    admin.exec("MERGE BRANCH dev ON CONFLICT PARENT WINS").await;
    // What the merge should have made, worked out row by row.
    let mut expected: Vec<Vec<String>> = before
        .into_iter()
        .filter(|r| r[0] != "1")
        .map(|mut r| {
            if r[0] == "5" {
                r[2] = (r[2].parse::<i64>().unwrap() + 100).to_string();
            }
            if r[0] == "7" {
                r[3] = "parent".into();
            }
            if r[0] == "9" && r[1] == "z" {
                r[3] = "theirs".into();
            }
            r
        })
        .collect();
    expected.push(row(&["9", "q", "900", "new"]));
    expected.push(row(&["0", "a", "0", "first"]));
    expected.push(row(&["8", "p", "800", "p"]));
    for (b, v) in [("a", "1"), ("b", "2"), ("c", "3"), ("d", "4")] {
        expected.push(row(&["3", b, v, "p"]));
    }
    expected.sort_by(|x, y| {
        (x[0].parse::<i64>().unwrap(), x[1].clone()).cmp(&(y[0].parse::<i64>().unwrap(), y[1].clone()))
    });
    assert_eq!(prod.rows("SELECT a, b, v, w FROM grid ORDER BY a, b").await, expected);
    assert!(admin.rows("SHOW BRANCH DIFF dev").await.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rows_both_sides_numbered_alike_are_renumbered_with_their_references() {
    let fx = Fixture::new("serial").await;
    let mut admin = fx.client("postgres");
    admin.exec("CREATE DATABASE prod").await;
    let mut prod = fx.client("prod");
    prod.exec("CREATE TABLE orders (id SERIAL PRIMARY KEY, what TEXT)").await;
    prod.exec("CREATE TABLE lines (id SERIAL PRIMARY KEY, order_id INT REFERENCES orders(id), qty INT)")
        .await;
    prod.exec("INSERT INTO orders (what) VALUES ('one'), ('two')").await;
    prod.exec("INSERT INTO lines (order_id, qty) VALUES (1, 1)").await;
    admin.exec("CREATE BRANCH dev FROM prod").await;

    // Both sides take the next numbers for different rows.
    let mut dev = fx.client("dev");
    dev.exec("INSERT INTO orders (what) VALUES ('dev-a'), ('dev-b')").await; // 3, 4
    dev.exec("INSERT INTO lines (order_id, qty) VALUES (3, 5), (4, 6)").await; // 2, 3
    dev.exec("UPDATE lines SET order_id = 4 WHERE id = 1").await;
    prod.exec("INSERT INTO orders (what) VALUES ('prod-x')").await; // 3
    prod.exec("INSERT INTO lines (order_id, qty) VALUES (3, 9)").await; // 2

    let diff = admin.rows("SHOW BRANCH DIFF dev").await;
    for t in ["orders", "lines"] {
        let r = diff.iter().find(|r| r[0] == t).unwrap_or_else(|| panic!("{diff:?}"));
        assert_eq!(r[5], "0", "{t} would conflict: {diff:?}");
    }
    admin.exec("MERGE BRANCH dev").await;
    let joined = "SELECT l.qty, o.what FROM lines l JOIN orders o ON o.id = l.order_id ORDER BY l.qty";
    assert_eq!(
        prod.rows(joined).await,
        vec![
            row(&["1", "dev-b"]),
            row(&["5", "dev-a"]),
            row(&["6", "dev-b"]),
            row(&["9", "prod-x"]),
        ]
    );
    assert_eq!(prod.scalar("SELECT COUNT(*) FROM orders").await, "5");
    // The parent's sequence moved past every row it holds.
    prod.exec("INSERT INTO orders (what) VALUES ('after')").await;
    assert_eq!(prod.scalar("SELECT COUNT(*) FROM orders").await, "6");
    assert!(admin.rows("SHOW BRANCH DIFF dev").await.is_empty());

    // Later, the branch changes rows it knows by their old numbers.
    let mut dev = fx.client("dev");
    dev.exec("UPDATE orders SET what = 'dev-a2' WHERE id = 3").await;
    dev.exec("UPDATE lines SET qty = 50 WHERE id = 2").await;
    dev.exec("DELETE FROM lines WHERE id = 3").await;
    let diff = admin.rows("SHOW BRANCH DIFF dev").await;
    let orders = diff.iter().find(|r| r[0] == "orders").unwrap_or_else(|| panic!("{diff:?}"));
    assert_eq!(&orders[2..6], &["0", "1", "0", "0"], "{diff:?}");
    let lines = diff.iter().find(|r| r[0] == "lines").unwrap_or_else(|| panic!("{diff:?}"));
    assert_eq!(&lines[2..6], &["0", "1", "1", "0"], "{diff:?}");
    admin.exec("MERGE BRANCH dev").await;
    assert_eq!(
        prod.rows(joined).await,
        vec![row(&["1", "dev-b"]), row(&["9", "prod-x"]), row(&["50", "dev-a2"])]
    );
    assert!(admin.rows("SHOW BRANCH DIFF dev").await.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rows_referencing_each_other_are_renumbered_in_order() {
    let fx = Fixture::new("serial_tree").await;
    let mut admin = fx.client("postgres");
    admin.exec("CREATE DATABASE prod").await;
    let mut prod = fx.client("prod");
    prod.exec("CREATE TABLE cats (id SERIAL PRIMARY KEY, parent_id INT REFERENCES cats(id), name TEXT)")
        .await;
    prod.exec("INSERT INTO cats (parent_id, name) VALUES (NULL, 'root')").await; // 1
    admin.exec("CREATE BRANCH dev FROM prod").await;
    let mut dev = fx.client("dev");
    dev.exec("INSERT INTO cats (parent_id, name) VALUES (1, 'fruit')").await; // 2
    dev.exec("INSERT INTO cats (parent_id, name) VALUES (2, 'apple')").await; // 3
    // An older row moved under a new one: inserted after it on the parent.
    dev.exec("INSERT INTO cats (parent_id, name) VALUES (NULL, 'pear')").await; // 4
    dev.exec("UPDATE cats SET parent_id = 4 WHERE id = 3").await;
    dev.exec("UPDATE cats SET parent_id = 2 WHERE id = 4").await;
    prod.exec("INSERT INTO cats (parent_id, name) VALUES (1, 'veg'), (1, 'nuts')").await; // 2, 3

    admin.exec("MERGE BRANCH dev").await;
    let tree = "SELECT c.name, p.name FROM cats c LEFT JOIN cats p ON p.id = c.parent_id ORDER BY c.name";
    assert_eq!(
        prod.rows(tree).await,
        vec![
            row(&["apple", "pear"]),
            row(&["fruit", "root"]),
            row(&["nuts", "root"]),
            row(&["pear", "fruit"]),
            row(&["root", "NULL"]),
            row(&["veg", "root"]),
        ]
    );
    assert!(admin.rows("SHOW BRANCH DIFF dev").await.is_empty());
}
