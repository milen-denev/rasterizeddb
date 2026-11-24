//! A path index (`CREATE INDEX ix ON t ((doc->>'k'))`) narrows a read of
//! `doc->>'k' = 'v'` to the rows it keys, and the narrowed read answers
//! exactly what the same read of an unindexed twin answers: documents whose
//! key holds a string, a number, a boolean, JSON null, an object, or is
//! missing, so `'7'` matches both `"7"` and `7` (what `->>` yields for each)
//! and a missing key or JSON null matches no equality. The same on a
//! hash-partitioned table (each partition keeps the index) and on a table
//! placed on a key/value store.

use std::path::Path;

use rclite::Arc;

use super::Database;

async fn open(dir: &Path) -> Arc<Database> {
    let _ = crate::MAX_PERMITS_THREADS.set(4);
    let _ = crate::BATCH_SIZE.set(512);
    let _ = crate::core::processor::concurrent_processor::ENABLE_CACHE.set(false);
    Arc::new(Database::try_new_named(&dir.to_string_lossy(), false, "path_prune").await.expect("open the database"))
}

fn close(dir: &Path, db: Arc<Database>) {
    drop(db);
    crate::core::wal::remove_for(&dir.to_string_lossy());
}

async fn exec(db: &Arc<Database>, sql: &str) -> Vec<u8> {
    let purpose = crate::core::rql::lexer_s1::recognize_query_purpose(sql).unwrap_or_else(|| panic!("parse {sql}"));
    let out = crate::core::rql::executor::execute_with_sync(purpose, db.clone(), true).await;
    assert!(!out.is_empty() && out[0] != 3, "{sql}: {}", String::from_utf8_lossy(out.get(1..).unwrap_or_default()));
    out
}

/// The document of row `i`: mostly a distinct string, with every other shape
/// the key can take mixed in.
fn doc(i: u64) -> String {
    match i % 10 {
        0 => format!("{{\"k\": {}}}", i / 10),
        1 => "{\"other\": 1}".to_string(),
        2 => "{\"k\": null}".to_string(),
        3 => format!("{{\"k\": {}}}", i % 3 == 0),
        4 => format!("{{\"k\": {{\"a\": {i}}}}}"),
        _ => format!("{{\"k\": \"v{}\"}}", i / 10),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_path_index_prunes_and_answers_as_a_scan() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().to_path_buf();
    let db = open(&dir).await;
    const ROWS: u64 = 20_000;
    let store = dir.join("store");
    std::fs::create_dir_all(&store).unwrap();
    crate::core::storage::sql::execute(
        &format!("STORAGE ADD STORE kv DIRECTORY WITH (PATH = '{}', PREFIX = 'kv')", store.to_string_lossy()),
        &db,
    )
    .await
    .unwrap();
    crate::core::storage::sql::execute("STORAGE PLACE TABLE jk AT 'kv:kv'", &db).await.unwrap();
    // `j` on files, `jp` partitioned, `jk` on the store, all indexed; `jt`
    // the unindexed twin they are compared with.
    for t in ["j", "jp", "jk", "jt"] {
        let create = if t == "jp" {
            "CREATE TABLE jp (id BIGINT NOT NULL, doc JSONB, PRIMARY KEY (id)) PARTITION BY HASH (id) PARTITIONS 8".to_string()
        } else {
            format!("CREATE TABLE {t} (id BIGINT PRIMARY KEY, doc JSONB)")
        };
        exec(&db, &create).await;
        let mut i = 0;
        while i < ROWS {
            let end = (i + 1000).min(ROWS);
            let values: Vec<String> = (i..end).map(|i| format!("({i}, '{}')", doc(i))).collect();
            exec(&db, &format!("INSERT INTO {t} (id, doc) VALUES {}", values.join(", "))).await;
            i = end;
        }
    }
    for t in ["j", "jp", "jk"] {
        exec(&db, &format!("CREATE INDEX {t}_k ON {t} ((doc->>'k'))")).await;
    }
    let ready = |t: &Arc<crate::core::row::table::Table<crate::core::storage_providers::backend::DbStorageProvider>>| {
        t.indexes().unwrap().is_none_or(|ix| ix.is_ready())
    };
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
    loop {
        let mut all = ["j", "jk"].iter().all(|t| ready(&db.table_for_tests(t).unwrap()));
        if let Some(pt) = db.partitioned_table("jp") {
            for (_, p) in pt.open_partitions_now() {
                all &= ready(&p);
            }
        }
        if all {
            break;
        }
        assert!(std::time::Instant::now() < deadline, "a path index never became ready");
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }

    let clauses = [
        "doc->>'k' = 'v7'",
        "doc->>'k' = '7'",
        "'v1234' = doc->>'k'",
        "doc->>'k' = 'true'",
        "doc->>'k' = 'nosuch'",
        "doc->>'k' = 'v7' OR doc->>'k' = 'v8'",
        "doc->>'k' = 'v7' AND id > 100",
        "doc->>'k' LIKE 'v12%'",
        "doc->>'k' IS NULL",
        "doc->>'k' <> 'v7'",
    ];
    for clause in clauses {
        let scanned = exec(&db, &format!("SELECT id, doc->>'k' FROM jt WHERE {clause} ORDER BY id")).await;
        for t in ["j", "jp", "jk"] {
            let indexed = exec(&db, &format!("SELECT id, doc->>'k' FROM {t} WHERE {clause} ORDER BY id")).await;
            assert_eq!(indexed, scanned, "{t}: {clause}: the indexed table answered differently");
        }
    }
    // The point clauses are narrowed to the rows the index keys, on files
    // and on the store alike.
    for t in ["j", "jk"] {
        let table = db.table_for_tests(t).unwrap();
        for clause in ["doc->>'k' = 'v7'", "doc->>'k' = '7'", "doc->>'k' = 'nosuch'"] {
            let candidates = table.build_candidates(clause, None).await.unwrap();
            let candidates = candidates.unwrap_or_else(|| panic!("{t}: {clause}: the path index narrowed nothing"));
            let listed = candidates.slots().len() as u64;
            assert!(listed <= 10, "{t}: {clause}: {listed} candidate slots of {ROWS}");
        }
    }
    close(&dir, db);
}
