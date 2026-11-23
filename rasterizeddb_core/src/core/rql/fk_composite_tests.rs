//! Composite foreign keys on disk: what a restart reads back, and the fence
//! that keeps a binary which would read one as a one-column key out of the
//! directory (`incompat::COMPOSITE_FOREIGN_KEY_V1`).

use crate::core::database::Database;
use crate::core::rql::executor::execute;
use crate::core::rql::lexer_s1::recognize_query_purpose;
use rclite::Arc;

fn init_globals() {
    let _ = crate::MAX_PERMITS_THREADS.set(4);
    let _ = crate::BATCH_SIZE.set(1024);
}

async fn open(dir: &std::path::Path) -> Arc<Database> {
    Arc::new(Database::new_named(&dir.to_string_lossy(), false, "fkcompositetests").await)
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

async fn run_ok(database: &Arc<Database>, sql: &str) {
    run(database, sql).await.unwrap_or_else(|e| panic!("`{sql}` failed: {e}"));
}

fn flags(dir: &std::path::Path) -> Vec<String> {
    crate::core::database::incompat::flags(dir).unwrap()
}

/// A composite key is written behind the fence and read back whole after a
/// restart: enforced on the tuple, both sides, and still composite in the
/// schema. A database with only one-column keys carries no fence.
#[tokio::test(flavor = "multi_thread")]
async fn a_composite_key_survives_a_restart_behind_its_fence() {
    init_globals();
    tokio::time::timeout(std::time::Duration::from_secs(300), async {
        let dir = tempfile::tempdir().unwrap();
        {
            let db = open(dir.path()).await;
            run_ok(&db, "CREATE TABLE p (x INT, y INT, PRIMARY KEY (x, y))").await;
            run_ok(&db, "INSERT INTO p (x, y) VALUES (1, 1), (1, 2), (2, 1)").await;
            run_ok(&db, "CREATE TABLE q (id INT PRIMARY KEY)").await;
            run_ok(&db, "CREATE TABLE one (id INT PRIMARY KEY, q INT REFERENCES q (id))").await;
            assert!(
                !flags(dir.path()).iter().any(|f| f == crate::core::database::incompat::COMPOSITE_FOREIGN_KEY_V1),
                "fenced before any composite key"
            );
            run_ok(&db, "CREATE TABLE c (id INT PRIMARY KEY, a INT, b INT, FOREIGN KEY (a, b) REFERENCES p)").await;
            assert!(flags(dir.path()).iter().any(|f| f == crate::core::database::incompat::COMPOSITE_FOREIGN_KEY_V1));
            run_ok(&db, "INSERT INTO c (id, a, b) VALUES (1, 1, 1), (2, 2, 1)").await;
            drop(db);
        }
        crate::core::wal::remove_for(&dir.path().to_string_lossy());
        let db = open(dir.path()).await;
        let keys = db.schema_view("c").unwrap().schema().foreign_keys.clone();
        assert_eq!(keys.len(), 1, "{keys:?}");
        assert_eq!((keys[0].columns.as_slice(), keys[0].parent_columns.as_slice()), (&["a".to_string(), "b".to_string()][..], &["x".to_string(), "y".to_string()][..]));
        assert_eq!(keys[0].name, "c_a_b_fkey");
        // (2, 2): both values are some parent's, the tuple is none's.
        let e = run(&db, "INSERT INTO c (id, a, b) VALUES (3, 2, 2)").await.unwrap_err();
        assert!(e.contains("key (a, b)=(2, 2) is not present in table \"p\""), "{e}");
        run_ok(&db, "INSERT INTO c (id, a, b) VALUES (3, 1, 2)").await;
        let e = run(&db, "DELETE FROM p WHERE x = 2").await.unwrap_err();
        assert!(e.contains("violates foreign key constraint \"c_a_b_fkey\" on table \"c\""), "{e}");
        // A parent sharing one value with a referenced one goes.
        run_ok(&db, "INSERT INTO p (x, y) VALUES (2, 9)").await;
        run_ok(&db, "DELETE FROM p WHERE x = 2 AND y = 9").await;
        // Dropped, the fence stays (another key may come back), and the key
        // is gone after a restart too.
        run_ok(&db, "ALTER TABLE c DROP CONSTRAINT c_a_b_fkey").await;
        run_ok(&db, "INSERT INTO c (id, a, b) VALUES (4, 7, 7)").await;
    })
    .await
    .expect("bounded");
}

/// A binary that does not know the fence refuses the directory, naming it:
/// what every build from 1.0.0-rc.1 on does with a flag it does not know.
#[test]
fn a_build_without_composite_keys_refuses_the_directory() {
    let dir = tempfile::tempdir().unwrap();
    crate::core::database::incompat::require(dir.path(), crate::core::database::incompat::COMPOSITE_FOREIGN_KEY_V1)
        .unwrap();
    assert_eq!(crate::core::database::incompat::check(dir.path()), Ok(()));
    let text = std::fs::read_to_string(dir.path().join(crate::core::database::incompat::FILE_NAME)).unwrap();
    assert!(text.lines().any(|l| l == "composite_foreign_key_v1"), "{text}");
}

/// A directory holding a composite key's records without the flag (a
/// restore, a replica: both bring the records and not the flag) is found by
/// the restore's scan, and fenced at open.
#[tokio::test(flavor = "multi_thread")]
async fn a_composite_key_without_its_fence_is_fenced_again() {
    init_globals();
    tokio::time::timeout(std::time::Duration::from_secs(300), async {
        let dir = tempfile::tempdir().unwrap();
        {
            let db = open(dir.path()).await;
            run_ok(&db, "CREATE TABLE p (x INT, y INT, PRIMARY KEY (x, y))").await;
            run_ok(&db, "CREATE TABLE c (a INT, b INT, FOREIGN KEY (a, b) REFERENCES p (x, y))").await;
            drop(db);
        }
        crate::core::wal::remove_for(&dir.path().to_string_lossy());
        let marker = dir.path().join(crate::core::database::incompat::FILE_NAME);
        std::fs::remove_file(&marker).unwrap();
        assert_eq!(
            crate::core::database::incompat::directories_needing_composite_foreign_keys(dir.path()).unwrap(),
            vec![dir.path().to_path_buf()]
        );
        let db = open(dir.path()).await;
        assert!(flags(dir.path()).iter().any(|f| f == crate::core::database::incompat::COMPOSITE_FOREIGN_KEY_V1));
        let e = run(&db, "INSERT INTO c (a, b) VALUES (1, 1)").await.unwrap_err();
        assert!(e.contains("violates foreign key constraint"), "{e}");
    })
    .await
    .expect("bounded");
}
