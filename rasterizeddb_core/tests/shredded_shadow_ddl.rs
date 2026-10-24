//! Shredded JSON shadow DDL against document writers (slice B5,
//! docs/persistent-indexes.md §7 "JSON shadow DDL", R4, Q4, finding L9,
//! V10 case 11).
//!
//! A new key extends a shadow table inside the writer's own INSERT. Two
//! blocks that have each written a document, each then inserting documents
//! with new keys, both finish with no deadlock; so does a writer whose
//! parent table has an ALTER queued behind it; one INSERT of
//! `[{"a":1},{"b":2}]` (keys first) and concurrent writers adding keys lose
//! no document.

use std::time::Duration;

use rasterizeddb_core::{
    BATCH_SIZE, MAX_PERMITS_THREADS,
    core::{
        database::Database,
        mvcc::{TxId, owner::LockOwner},
        rql::{
            executor::{execute, execute_in_txn},
            lexer_s1::recognize_query_purpose,
        },
    },
};
use rclite::Arc;

/// A big stack, as `json_in_transaction.rs` explains.
fn on_a_big_stack<F: std::future::Future<Output = ()> + Send + 'static>(body: F) {
    std::thread::Builder::new()
        .stack_size(32 * 1024 * 1024)
        .spawn(move || {
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(4)
                .thread_stack_size(32 * 1024 * 1024)
                .enable_all()
                .build()
                .expect("a tokio runtime")
                .block_on(body);
        })
        .expect("a test thread")
        .join()
        .expect("the test thread panicked");
}

fn init_globals() {
    MAX_PERMITS_THREADS.get_or_init(|| 4);
    BATCH_SIZE.get_or_init(|| 1_000);
    // The shredded mapping is what has shadow DDL. Process-wide; this file
    // is its own process.
    let _ = rasterizeddb_core::JSON_SHADOW_TABLES.set(true);
}

async fn run(db: &Arc<Database>, q: &str) -> Result<String, String> {
    let purpose = recognize_query_purpose(q).expect("recognized");
    let out = execute(purpose, db.clone()).await;
    if out.first() == Some(&3) {
        return Err(String::from_utf8_lossy(&out[1..]).to_string());
    }
    Ok(String::from_utf8_lossy(&out).to_string())
}

async fn run_in(db: &Arc<Database>, q: &str, tx: TxId) -> Result<(), String> {
    let purpose = recognize_query_purpose(q).expect("recognized");
    let out = execute_in_txn(purpose, db.clone(), tx).await;
    if out.first() == Some(&3) {
        return Err(String::from_utf8_lossy(&out[1..]).to_string());
    }
    Ok(())
}

/// Ends a block as the wire does: durable, committed, its owner released.
fn commit(db: &Arc<Database>, tx: TxId) {
    db.sync_durable().expect("durable");
    db.commit_transaction(tx);
    db.transactions.flush().expect("flushed");
    db.release_owner(LockOwner::block(tx).expect("a block's owner"));
}

const BOUND: Duration = Duration::from_secs(20);

async fn doc_of(db: &Arc<Database>, id: i64) -> String {
    run(db, &format!("SELECT doc FROM p WHERE id = {id}")).await.expect("read back")
}

/// V10 case 11, blocks: two blocks have each written a document into one
/// shredded column (each holds ROW EXCLUSIVE on the parent), then each
/// inserts documents with keys the shadow has never seen, at once. Both
/// finish, no 40P01, and every document reads back whole. A SELECT of the
/// column meanwhile does not wait.
///
/// Mutation: shadow DDL takes ACCESS EXCLUSIVE on the parent through the
/// table-lock manager (shadow ADD COLUMN "under AE"): the two blocks wait for
/// each other's ROW EXCLUSIVE and one gets 40P01.
#[test]
fn two_blocks_adding_keys_to_one_shadow_finish_without_a_deadlock() {
    on_a_big_stack(two_blocks_body());
}

async fn two_blocks_body() {
    init_globals();
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(Database::new_with_backend(&dir.path().to_string_lossy(), false).await);
    run(&db, "CREATE TABLE p (id INT NOT NULL, doc JSONB NULL)").await.unwrap();
    run(&db, r#"INSERT INTO p (id, doc) VALUES (0, '{"base":0}')"#).await.unwrap();

    let (t1, t2) = (db.transactions.begin(), db.transactions.begin());
    run_in(&db, r#"INSERT INTO p (id, doc) VALUES (1, '{"base":1}')"#, t1).await.unwrap();
    run_in(&db, r#"INSERT INTO p (id, doc) VALUES (2, '{"base":2}')"#, t2).await.unwrap();

    let writer = |tx: TxId, id: i64, key: &'static str| {
        let db = db.clone();
        tokio::spawn(async move {
            for k in 0..3 {
                let id = id * 10 + k;
                let sql = format!(r#"INSERT INTO p (id, doc) VALUES ({id}, '{{"{key}{k}":{id},"base":{id}}}')"#);
                run_in(&db, &sql, tx).await?;
            }
            Ok::<(), String>(())
        })
    };
    let (w1, w2) = (writer(t1, 1, "a"), writer(t2, 2, "b"));
    let reader = {
        let db = db.clone();
        tokio::spawn(async move { run(&db, "SELECT id FROM p WHERE id = 0").await })
    };
    let r = tokio::time::timeout(Duration::from_secs(5), reader)
        .await
        .expect("a SELECT of the column waited for the writers' shadow DDL")
        .unwrap();
    r.expect("the SELECT failed");
    for (name, w) in [("first", w1), ("second", w2)] {
        let out = tokio::time::timeout(BOUND, w)
            .await
            .unwrap_or_else(|_| panic!("the {name} block's new-key inserts did not finish"))
            .unwrap();
        out.unwrap_or_else(|e| panic!("the {name} block's new-key insert failed (zero 40P01 expected): {e}"));
    }
    commit(&db, t1);
    commit(&db, t2);
    for (id, key) in [(10, "a0"), (11, "a1"), (12, "a2"), (20, "b0"), (21, "b1"), (22, "b2")] {
        let doc = doc_of(&db, id).await;
        assert!(
            doc.contains(&format!("\"{key}\":{id}")) && doc.contains(&format!("\"base\":{id}")),
            "document {id} lost a key: {doc}"
        );
    }
    for id in [0, 1, 2] {
        let doc = doc_of(&db, id).await;
        assert!(doc.contains(&format!("\"base\":{id}")), "document {id} was lost: {doc}");
    }
}

/// Finding L9, end to end: a block has written a document (ROW EXCLUSIVE on
/// the parent), an `ALTER TABLE` of the parent queues behind it, and the
/// block then inserts a document with a new key: its shadow DDL does not
/// queue behind the pending ALTER (which waits for the block), so it
/// finishes; the block commits, and the ALTER runs.
///
/// Mutation: the shadow lock yields to a pending AE on the parent; the
/// insert waits for the ALTER, which waits for the insert's block.
#[test]
fn a_writers_shadow_ddl_does_not_queue_behind_a_pending_parent_alter() {
    on_a_big_stack(pending_alter_body());
}

async fn pending_alter_body() {
    init_globals();
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(Database::new_with_backend(&dir.path().to_string_lossy(), false).await);
    run(&db, "CREATE TABLE p (id INT NOT NULL, doc JSONB NULL)").await.unwrap();
    let tx = db.transactions.begin();
    run_in(&db, r#"INSERT INTO p (id, doc) VALUES (1, '{"a":1}')"#, tx).await.unwrap();
    let alter = {
        let db = db.clone();
        tokio::spawn(async move { run(&db, "ALTER TABLE p ADD COLUMN z INT").await })
    };
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert!(!alter.is_finished(), "the ALTER ran beside a block that wrote the table");
    tokio::time::timeout(
        Duration::from_secs(10),
        run_in(&db, r#"INSERT INTO p (id, doc) VALUES (2, '{"a":2,"fresh":true}')"#, tx),
    )
    .await
    .expect("the block's new-key insert queued behind the pending ALTER")
    .expect("the block's new-key insert failed");
    commit(&db, tx);
    tokio::time::timeout(BOUND, alter)
        .await
        .expect("the ALTER did not run once the block ended")
        .unwrap()
        .expect("the ALTER failed");
    let doc = doc_of(&db, 2).await;
    assert!(doc.contains("\"fresh\":true"), "{doc}");
}

/// Keys first: one INSERT of an array whose elements bring different keys
/// never rewrites a shadow holding its own rows, and reads back exactly.
#[test]
fn one_insert_of_elements_with_different_keys_reads_back_whole() {
    on_a_big_stack(async {
        init_globals();
        let dir = tempfile::tempdir().unwrap();
        let db = Arc::new(Database::new_with_backend(&dir.path().to_string_lossy(), false).await);
        run(&db, "CREATE TABLE p (id INT NOT NULL, doc JSONB NULL)").await.unwrap();
        run(&db, r#"INSERT INTO p (id, doc) VALUES (1, '[{"a":1},{"b":2},{"a":3.5,"c":"x"}]')"#)
            .await
            .unwrap();
        let doc = doc_of(&db, 1).await;
        for part in ["\"a\":1", "\"b\":2", "\"a\":3.5", "\"c\":\"x\""] {
            assert!(doc.contains(part), "the array lost {part}: {doc}");
        }
    });
}

/// Autocommit writers adding keys to one shadow at once: every document
/// reads back whole. Before the latch a writer's shadow row could land while
/// another writer's ADD COLUMN rewrote the shadow, which did not carry it.
#[test]
fn concurrent_writers_adding_keys_lose_no_document() {
    on_a_big_stack(async {
        init_globals();
        let dir = tempfile::tempdir().unwrap();
        let db = Arc::new(Database::new_with_backend(&dir.path().to_string_lossy(), false).await);
        run(&db, "CREATE TABLE p (id INT NOT NULL, doc JSONB NULL)").await.unwrap();
        run(&db, r#"INSERT INTO p (id, doc) VALUES (0, '{"seed":0}')"#).await.unwrap();
        let mut tasks = Vec::new();
        for w in 0..3i64 {
            let db = db.clone();
            tasks.push(tokio::spawn(async move {
                for k in 0..8i64 {
                    let id = 100 + w * 100 + k;
                    let sql = format!(r#"INSERT INTO p (id, doc) VALUES ({id}, '{{"w{w}k{k}":{id}}}')"#);
                    run(&db, &sql).await?;
                }
                Ok::<(), String>(())
            }));
        }
        for t in tasks {
            tokio::time::timeout(BOUND, t).await.expect("a writer hung").unwrap().expect("a writer failed");
        }
        for w in 0..3i64 {
            for k in 0..8i64 {
                let id = 100 + w * 100 + k;
                let doc = doc_of(&db, id).await;
                assert!(doc.contains(&format!("\"w{w}k{k}\":{id}")), "document {id} came back wrong: {doc}");
            }
        }
    });
}
