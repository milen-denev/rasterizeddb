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

fn count_of(response: &[u8]) -> usize {
    crate::core::row::row::vec_into_rows(&response[1..]).expect("rows").len()
}

async fn count(database: &Arc<Database>, sql: &str) -> usize {
    match run(database, sql).await {
        Ok(response) => count_of(&response),
        Err(e) => panic!("`{sql}` failed: {e}"),
    }
}

#[track_caller]
fn refused_with(result: Result<Vec<u8>, String>, sql: &str, sqlstate: &str, text: &str) {
    let e = match result {
        Ok(_) => panic!("`{sql}` was accepted; PostgreSQL 17 refuses it with {sqlstate}"),
        Err(e) => e,
    };
    assert_eq!(crate::pgwire::sqlstate_for(&e), sqlstate, "{sql}: {e}");
    assert!(e.contains(text), "{sql}: {e}");
}

async fn refused(database: &Arc<Database>, sql: &str, sqlstate: &str, text: &str) {
    refused_with(run(database, sql).await, sql, sqlstate, text);
}

/// The `pms` layout: `step` and `field` hash-partitioned by `(domain_id,
/// case_id)`, `case` plain; `field` → `step` partitioned onto partitioned
/// (co-located), `step` → `case` partitioned onto plain.
async fn pms(db: &Arc<Database>) {
    run_ok(db, "CREATE TABLE \"case\" (domain_id BIGINT NOT NULL, id BIGINT NOT NULL, PRIMARY KEY (domain_id, id))").await;
    run_ok(
        db,
        "CREATE TABLE step (domain_id BIGINT NOT NULL, case_id BIGINT NOT NULL, id BIGINT NOT NULL, \
         PRIMARY KEY (domain_id, case_id, id)) PARTITION BY HASH (domain_id, case_id) PARTITIONS 4",
    )
    .await;
    run_ok(
        db,
        "CREATE TABLE field (domain_id BIGINT NOT NULL, case_id BIGINT NOT NULL, step_id BIGINT NOT NULL, \
         id BIGINT NOT NULL, PRIMARY KEY (domain_id, case_id, step_id, id)) PARTITION BY HASH (domain_id, case_id) \
         PARTITIONS 4",
    )
    .await;
    run_ok(db, "INSERT INTO \"case\" VALUES (1, 10), (1, 11), (2, 10), (-3, 7)").await;
    run_ok(db, "INSERT INTO step VALUES (1, 10, 100), (1, 10, 101), (2, 10, 100), (-3, 7, 1)").await;
    run_ok(db, "INSERT INTO field VALUES (1, 10, 100, 1), (-3, 7, 1, 1)").await;
    // Validated against the rows already there, in every partition.
    run_ok(
        db,
        "ALTER TABLE field ADD CONSTRAINT field_step_fk FOREIGN KEY (domain_id, case_id, step_id) \
         REFERENCES step (domain_id, case_id, id)",
    )
    .await;
    run_ok(
        db,
        "ALTER TABLE step ADD CONSTRAINT step_case_fk FOREIGN KEY (domain_id, case_id) REFERENCES \"case\" (domain_id, id)",
    )
    .await;
}

/// Composite keys on and onto hash-partitioned tables, the `pms` shapes
/// first: each answer is PostgreSQL 17's (declarative partitions, `MODULUS
/// 4`), every check on the tuple, every write path, both sides' actions, a
/// key onto the table itself, and all of it again after a restart.
#[tokio::test(flavor = "multi_thread")]
async fn composite_keys_hold_on_and_onto_partitioned_tables() {
    init_globals();
    tokio::time::timeout(std::time::Duration::from_secs(300), async {
        let dir = tempfile::tempdir().unwrap();
        {
            let db = open(dir.path()).await;
            pms(&db).await;
            assert!(flags(dir.path()).iter().any(|f| f == crate::core::database::incompat::COMPOSITE_FOREIGN_KEY_V1));
            // ADD validates every partition.
            run_ok(&db, "CREATE TABLE f2 (domain_id BIGINT NOT NULL, case_id BIGINT NOT NULL, step_id BIGINT NOT NULL, \
                         PRIMARY KEY (domain_id, case_id, step_id)) PARTITION BY HASH (domain_id, case_id) PARTITIONS 4").await;
            run_ok(&db, "INSERT INTO f2 VALUES (1, 10, 100), (2, 10, 101)").await;
            refused(&db, "ALTER TABLE f2 ADD FOREIGN KEY (domain_id, case_id, step_id) REFERENCES step", "23503",
                "key (domain_id, case_id, step_id)=(2, 10, 101) is not present in table \"step\"").await;
            run_ok(&db, "DROP TABLE f2").await;

            // The child side: partitioned onto plain, partitioned onto
            // partitioned; each value of the tuple some parent's, the tuple none's.
            refused(&db, "INSERT INTO step VALUES (2, 11, 1)", "23503", "key (domain_id, case_id)=(2, 11) is not present in table \"case\"").await;
            refused(&db, "INSERT INTO step VALUES (1, 12, 1)", "23503", "step_case_fk").await;
            refused(&db, "INSERT INTO step VALUES (1, 11, 5), (9, 9, 9)", "23503", "(9, 9)").await;
            run_ok(&db, "INSERT INTO step VALUES (1, 11, 5), (1, 11, 6)").await;
            run_ok(&db, "INSERT INTO field VALUES (1, 10, 101, 1)").await;
            refused(&db, "INSERT INTO field VALUES (1, 10, 102, 1)", "23503",
                "key (domain_id, case_id, step_id)=(1, 10, 102) is not present in table \"step\"").await;
            refused(&db, "INSERT INTO field VALUES (2, 10, 101, 1)", "23503", "field_step_fk").await;
            // ON CONFLICT: skipped rows are not checked; an update is.
            run_ok(&db, "INSERT INTO field VALUES (1, 10, 100, 2) ON CONFLICT DO NOTHING").await;
            refused(&db, "INSERT INTO field VALUES (1, 10, 100, 1) ON CONFLICT (domain_id, case_id, step_id, id) \
                          DO UPDATE SET step_id = 101", "23505", "duplicate key").await;
            refused(&db, "INSERT INTO field VALUES (1, 10, 100, 1) ON CONFLICT (domain_id, case_id, step_id, id) \
                          DO UPDATE SET step_id = 999", "23503", "(1, 10, 999)").await;
            // INSERT … SELECT.
            run_ok(&db, "INSERT INTO field SELECT domain_id, case_id, id, 50 FROM step").await;
            assert_eq!(count(&db, "SELECT id FROM field WHERE id = 50").await, 6);
            run_ok(&db, "DELETE FROM field WHERE id = 50").await;
            refused(&db, "INSERT INTO field SELECT domain_id, case_id, id + 1000, 50 FROM step WHERE domain_id = 1",
                "23503", "field_step_fk").await;
            // UPDATE: a column of the tuple, then the partition key itself
            // (the row moves to another partition, its new tuple checked).
            refused(&db, "UPDATE field SET step_id = 777 WHERE domain_id = 1 AND id = 2", "23503", "(1, 10, 777)").await;
            run_ok(&db, "UPDATE field SET step_id = 101 WHERE domain_id = 1 AND id = 2").await;
            refused(&db, "UPDATE field SET case_id = 11 WHERE domain_id = -3", "23503", "(-3, 11, 1)").await;
            refused(&db, "INSERT INTO step VALUES (-3, 11, 1)", "23503", "(-3, 11)").await;
            run_ok(&db, "INSERT INTO \"case\" VALUES (-3, 11)").await;
            run_ok(&db, "INSERT INTO step VALUES (-3, 11, 1)").await;
            run_ok(&db, "UPDATE field SET case_id = 11 WHERE domain_id = -3").await;
            assert_eq!(count(&db, "SELECT id FROM field WHERE domain_id = -3 AND case_id = 11").await, 1);

            // The parent side: NO ACTION on a partitioned parent and on a
            // plain parent of a partitioned child.
            refused(&db, "DELETE FROM step WHERE domain_id = 1 AND case_id = 10 AND id = 100", "23503",
                "update or delete on table \"step\" violates foreign key constraint \"field_step_fk\" on table \"field\"").await;
            refused(&db, "UPDATE step SET id = 300 WHERE domain_id = 1 AND case_id = 10 AND id = 101", "23503", "field_step_fk").await;
            run_ok(&db, "DELETE FROM step WHERE domain_id = 2").await;
            refused(&db, "DELETE FROM \"case\" WHERE domain_id = 1 AND id = 10", "23503",
                "violates foreign key constraint \"step_case_fk\" on table \"step\"").await;
            refused(&db, "UPDATE \"case\" SET id = 12 WHERE domain_id = 1 AND id = 11", "23503", "step_case_fk").await;
            run_ok(&db, "DELETE FROM \"case\" WHERE domain_id = 2 AND id = 10").await;
            run_ok(&db, "UPDATE step SET id = 300 WHERE domain_id = 1 AND case_id = 11 AND id = 5").await;

            // A plain child onto the partitioned parent, CASCADE both ways.
            run_ok(&db, "CREATE TABLE note (n INT PRIMARY KEY, domain_id BIGINT, case_id BIGINT, step_id BIGINT, \
                         FOREIGN KEY (domain_id, case_id, step_id) REFERENCES step ON DELETE CASCADE ON UPDATE CASCADE)").await;
            run_ok(&db, "INSERT INTO note VALUES (1, 1, 10, 100), (2, 1, 11, 300), (3, NULL, 99, 99), (5, 1, 11, 6)").await;
            refused(&db, "INSERT INTO note VALUES (4, 1, 11, 100)", "23503", "(1, 11, 100)").await;
            run_ok(&db, "UPDATE step SET id = 301 WHERE domain_id = 1 AND case_id = 11 AND id = 300").await;
            assert_eq!(count(&db, "SELECT n FROM note WHERE n = 2 AND step_id = 301").await, 1, "cascaded");
            run_ok(&db, "DELETE FROM step WHERE domain_id = 1 AND case_id = 11").await;
            assert_eq!(count(&db, "SELECT n FROM note").await, 2, "notes 2 and 5 cascaded");

            // A partitioned child declared inline onto a plain parent: its
            // CASCADE moves rows between partitions (the key's partition key).
            refused(&db, "CREATE TABLE bad (a BIGINT, b BIGINT, FOREIGN KEY (a, b) REFERENCES step (case_id, id))",
                "42830", "there is no unique constraint matching given keys for referenced table \"step\"").await;
            run_ok(&db, "CREATE TABLE ev (domain_id BIGINT NOT NULL, case_id BIGINT NOT NULL, e BIGINT NOT NULL, \
                         PRIMARY KEY (domain_id, case_id, e), CONSTRAINT ev_case_fk FOREIGN KEY (domain_id, case_id) \
                         REFERENCES \"case\" (domain_id, id) ON DELETE CASCADE ON UPDATE CASCADE) \
                         PARTITION BY HASH (domain_id, case_id) PARTITIONS 4").await;
            run_ok(&db, "INSERT INTO \"case\" VALUES (4, 1), (4, 2)").await;
            run_ok(&db, "INSERT INTO ev VALUES (4, 1, 1), (4, 1, 2), (4, 2, 1)").await;
            refused(&db, "INSERT INTO ev VALUES (1, 99, 1)", "23503", "ev_case_fk").await;
            run_ok(&db, "UPDATE \"case\" SET id = 20 WHERE domain_id = 4 AND id = 1").await;
            assert_eq!(count(&db, "SELECT e FROM ev WHERE domain_id = 4 AND case_id = 20").await, 2, "moved");
            assert_eq!(count(&db, "SELECT e FROM ev WHERE domain_id = 4 AND case_id = 1").await, 0);
            run_ok(&db, "DELETE FROM \"case\" WHERE domain_id = 4").await;
            assert_eq!(count(&db, "SELECT e FROM ev").await, 0, "cascaded");

            // MATCH FULL and ON DELETE SET NULL (c) on a partitioned child.
            run_ok(&db, "CREATE TABLE sn (k BIGINT NOT NULL, id BIGINT NOT NULL, d BIGINT, c BIGINT, PRIMARY KEY (k, id), \
                         FOREIGN KEY (d, c) REFERENCES \"case\" (domain_id, id) MATCH FULL ON DELETE SET NULL (c)) \
                         PARTITION BY HASH (k) PARTITIONS 4").await;
            refused(&db, "INSERT INTO sn VALUES (1, 1, 1, NULL)", "23503",
                "MATCH FULL does not allow mixing of null and nonnull key values").await;
            run_ok(&db, "INSERT INTO sn VALUES (1, 1, NULL, NULL)").await;
            run_ok(&db, "CREATE TABLE sd (k BIGINT NOT NULL, id BIGINT NOT NULL, d BIGINT, c BIGINT, PRIMARY KEY (k, id), \
                         FOREIGN KEY (d, c) REFERENCES \"case\" (domain_id, id) ON DELETE SET NULL (c)) \
                         PARTITION BY HASH (k) PARTITIONS 4").await;
            run_ok(&db, "INSERT INTO \"case\" VALUES (5, 5), (5, 6)").await;
            run_ok(&db, "INSERT INTO sd VALUES (1, 1, 5, 5), (2, 2, 5, 6)").await;
            run_ok(&db, "DELETE FROM \"case\" WHERE domain_id = 5 AND id = 5").await;
            assert_eq!(count(&db, "SELECT k FROM sd WHERE k = 1 AND d = 5 AND c IS NULL").await, 1);
            assert_eq!(count(&db, "SELECT k FROM sd WHERE k = 2 AND d = 5 AND c = 6").await, 1);

            // A partitioned table onto itself: checked at the statement's end.
            run_ok(&db, "CREATE TABLE tree (d BIGINT NOT NULL, id BIGINT NOT NULL, parent BIGINT, PRIMARY KEY (d, id), \
                         FOREIGN KEY (d, parent) REFERENCES tree (d, id)) PARTITION BY HASH (d) PARTITIONS 4").await;
            run_ok(&db, "INSERT INTO tree VALUES (1, 4, 3), (1, 3, 1), (1, 1, NULL)").await;
            refused(&db, "INSERT INTO tree VALUES (1, 5, 9)", "23503", "key (d, parent)=(1, 9) is not present in table \"tree\"").await;
            refused(&db, "DELETE FROM tree WHERE d = 1 AND id = 1", "23503", "tree_d_parent_fkey").await;
            run_ok(&db, "DELETE FROM tree WHERE d = 1").await;
            assert_eq!(count(&db, "SELECT id FROM tree").await, 0);
            run_ok(&db, "INSERT INTO tree VALUES (2, 1, NULL), (2, 2, 1)").await;
            drop(db);
        }
        // After a restart: every key read back whole, still enforced.
        crate::core::wal::remove_for(&dir.path().to_string_lossy());
        let db = open(dir.path()).await;
        let keys = db.schema_view("field").unwrap().schema().foreign_keys.clone();
        assert_eq!(keys.len(), 1, "{keys:?}");
        assert_eq!(keys[0].parent_columns, ["domain_id", "case_id", "id"]);
        refused(&db, "INSERT INTO field VALUES (1, 10, 102, 1)", "23503", "field_step_fk").await;
        refused(&db, "INSERT INTO step VALUES (2, 11, 1)", "23503", "step_case_fk").await;
        refused(&db, "DELETE FROM step WHERE domain_id = 1 AND case_id = 10", "23503", "field_step_fk").await;
        refused(&db, "INSERT INTO tree VALUES (2, 3, 7)", "23503", "tree_d_parent_fkey").await;
        refused(&db, "DELETE FROM tree WHERE d = 2 AND id = 1", "23503", "tree_d_parent_fkey").await;
        run_ok(&db, "INSERT INTO field VALUES (1, 10, 100, 9)").await;
    })
    .await
    .expect("bounded");
}

/// DDL around composite keys on partitioned tables, both sides, as
/// PostgreSQL 17 answers it: RENAME COLUMN and RENAME TO carry the key,
/// DROP COLUMN of a referenced column is 2BP01 and of a referencing one
/// drops the key, DROP CONSTRAINT drops it, DROP TABLE is 2BP01 and with
/// CASCADE drops the keys naming it.
#[tokio::test(flavor = "multi_thread")]
async fn composite_keys_on_partitioned_tables_follow_their_ddl() {
    init_globals();
    tokio::time::timeout(std::time::Duration::from_secs(300), async {
        let dir = tempfile::tempdir().unwrap();
        let db = open(dir.path()).await;
        pms(&db).await;
        run_ok(&db, "CREATE TABLE note (n INT PRIMARY KEY, domain_id BIGINT, case_id BIGINT, step_id BIGINT, \
                     FOREIGN KEY (domain_id, case_id, step_id) REFERENCES step ON DELETE CASCADE)").await;
        run_ok(&db, "ALTER TABLE step RENAME COLUMN case_id TO cid").await;
        refused(&db, "INSERT INTO field VALUES (1, 10, 555, 1)", "23503", "field_step_fk").await;
        run_ok(&db, "INSERT INTO field VALUES (1, 10, 101, 1)").await;
        run_ok(&db, "ALTER TABLE step RENAME COLUMN cid TO case_id").await;
        run_ok(&db, "ALTER TABLE field RENAME COLUMN step_id TO sid").await;
        refused(&db, "INSERT INTO field VALUES (1, 10, 555, 2)", "23503", "field_step_fk").await;
        run_ok(&db, "ALTER TABLE field RENAME COLUMN sid TO step_id").await;
        run_ok(&db, "ALTER TABLE step RENAME TO stp").await;
        refused(&db, "INSERT INTO field VALUES (1, 10, 555, 1)", "23503", "is not present in table \"stp\"").await;
        refused(&db, "DELETE FROM stp WHERE domain_id = 1", "23503", "field_step_fk").await;
        run_ok(&db, "ALTER TABLE stp RENAME TO step").await;
        run_ok(&db, "ALTER TABLE field RENAME TO fld").await;
        refused(&db, "INSERT INTO fld VALUES (1, 10, 555, 1)", "23503", "field_step_fk").await;
        run_ok(&db, "ALTER TABLE fld RENAME TO field").await;
        refused(&db, "ALTER TABLE step DROP COLUMN id", "2BP01", "").await;
        refused(&db, "ALTER TABLE \"case\" DROP COLUMN id", "2BP01", "").await;
        refused(&db, "DROP TABLE step", "2BP01", "cannot drop table step because other objects depend on it").await;
        // The referencing column goes, and the key with it.
        run_ok(&db, "ALTER TABLE field DROP COLUMN step_id").await;
        run_ok(&db, "INSERT INTO field VALUES (1, 10, 9)").await;
        assert!(db.schema_view("field").unwrap().schema().foreign_keys.iter().all(|k| k.is_deleted));
        run_ok(&db, "DROP TABLE step CASCADE").await;
        run_ok(&db, "INSERT INTO note VALUES (1, 7, 7, 7)").await;
        assert!(db.schema_view("note").unwrap().schema().foreign_keys.iter().all(|k| k.is_deleted));
        run_ok(&db, "CREATE TABLE ev (domain_id BIGINT NOT NULL, case_id BIGINT NOT NULL, PRIMARY KEY (domain_id, case_id), \
                     CONSTRAINT ev_case_fk FOREIGN KEY (domain_id, case_id) REFERENCES \"case\" (domain_id, id)) \
                     PARTITION BY HASH (domain_id) PARTITIONS 4").await;
        refused(&db, "INSERT INTO ev VALUES (1, 999)", "23503", "ev_case_fk").await;
        refused(&db, "DROP TABLE \"case\"", "2BP01", "").await;
        run_ok(&db, "ALTER TABLE ev DROP CONSTRAINT ev_case_fk").await;
        run_ok(&db, "INSERT INTO ev VALUES (1, 999)").await;
        run_ok(&db, "DROP TABLE \"case\"").await;
    })
    .await
    .expect("bounded");
}

/// A table holding composite keys converted to a partitioned one (`ALTER
/// TABLE … PARTITION BY`, `CREATE INDEX … USING partition`), as child and
/// as parent, keeps them; so do the reshapes and a partitioned rewrite,
/// and a restart after them.
#[tokio::test(flavor = "multi_thread")]
async fn composite_keys_survive_conversion_reshapes_and_rewrites() {
    init_globals();
    tokio::time::timeout(std::time::Duration::from_secs(300), async {
        let dir = tempfile::tempdir().unwrap();
        {
            let db = open(dir.path()).await;
            run_ok(&db, "CREATE TABLE p (x BIGINT NOT NULL, y BIGINT NOT NULL, PRIMARY KEY (x, y))").await;
            run_ok(&db, "INSERT INTO p VALUES (1, 1), (1, 2), (2, 1), (-7, 3)").await;
            run_ok(&db, "CREATE TABLE qc (id BIGINT NOT NULL PRIMARY KEY, a BIGINT, b BIGINT, \
                         FOREIGN KEY (a, b) REFERENCES p ON DELETE CASCADE)").await;
            run_ok(&db, "CREATE TABLE qd (id BIGINT NOT NULL PRIMARY KEY, a BIGINT, b BIGINT, \
                         FOREIGN KEY (a, b) REFERENCES p)").await;
            run_ok(&db, "INSERT INTO qc VALUES (1, 1, 1), (2, 2, 1), (3, -7, 3), (4, 1, 2)").await;
            run_ok(&db, "INSERT INTO qd VALUES (1, 1, 2)").await;
            // The child, then the parent (by a key its referenced columns hold).
            run_ok(&db, "ALTER TABLE qc PARTITION BY HASH (id) PARTITIONS 4").await;
            assert!(db.partitioned_table("qc").is_some());
            run_ok(&db, "CREATE INDEX p_partition_key ON p USING partition (x) WITH (partitions = 4)").await;
            assert!(db.partitioned_table("p").is_some());
            for (sql, text) in [
                ("INSERT INTO qc VALUES (5, 2, 2)", "key (a, b)=(2, 2) is not present in table \"p\""),
                ("INSERT INTO qd VALUES (2, 2, 2)", "qd_a_b_fkey"),
                ("DELETE FROM p WHERE x = 1 AND y = 2", "qd_a_b_fkey"),
            ] {
                refused(&db, sql, "23503", text).await;
            }
            run_ok(&db, "INSERT INTO qc VALUES (5, -7, 3)").await;
            run_ok(&db, "DELETE FROM p WHERE x = -7").await;
            assert_eq!(count(&db, "SELECT id FROM qc WHERE a = -7").await, 0, "cascaded");
            // Reshapes of both: the keys still route and hold.
            run_ok(&db, "ALTER TABLE p SPLIT PARTITION FOR (x = 1)").await;
            run_ok(&db, "ALTER TABLE p ISOLATE KEY (x = 2)").await;
            run_ok(&db, "ALTER TABLE qc SPLIT PARTITION FOR (id = 1)").await;
            refused(&db, "INSERT INTO qc VALUES (6, 2, 2)", "23503", "qc_a_b_fkey").await;
            run_ok(&db, "INSERT INTO qc VALUES (6, 2, 1)").await;
            refused(&db, "DELETE FROM p WHERE x = 1 AND y = 2", "23503", "qd_a_b_fkey").await;
            // A rewrite of the populated partitioned child (phase 6 P8).
            run_ok(&db, "ALTER TABLE qc ADD COLUMN note TEXT DEFAULT 'n'").await;
            refused(&db, "INSERT INTO qc (id, a, b) VALUES (7, 2, 2)", "23503", "qc_a_b_fkey").await;
            run_ok(&db, "DELETE FROM p WHERE x = 2").await;
            assert_eq!(count(&db, "SELECT id FROM qc WHERE a = 2").await, 0, "cascaded after the rewrite");
            drop(db);
        }
        crate::core::wal::remove_for(&dir.path().to_string_lossy());
        let db = open(dir.path()).await;
        assert!(db.partitioned_table("qc").is_some() && db.partitioned_table("p").is_some());
        refused(&db, "INSERT INTO qc (id, a, b) VALUES (8, 2, 2)", "23503", "qc_a_b_fkey").await;
        refused(&db, "DELETE FROM p WHERE x = 1 AND y = 2", "23503", "qd_a_b_fkey").await;
        run_ok(&db, "DELETE FROM p WHERE x = 1 AND y = 1").await;
        assert_eq!(count(&db, "SELECT id FROM qc WHERE a = 1 AND b = 1").await, 0, "cascaded after a restart");
    })
    .await
    .expect("bounded");
}
