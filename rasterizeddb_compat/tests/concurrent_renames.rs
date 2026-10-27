//! Plain and partitioned renames at once: each finishes, every table keeps
//! its rows under its final name, and no intent is left behind.
//!
//! A plain rename's intent is `__rename.intent`, a partitioned one's
//! `__prename.intent`; both read the names an unfinished rename holds before
//! writing theirs. One rename at a time in a database (`rename_serial`),
//! whichever kind.

use rasterizeddb_compat::TestServer;
use sqlx::{AssertSqlSafe, Executor, Row};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn plain_and_partitioned_renames_interleave_cleanly() {
    const ROUNDS: usize = 6;
    let dir = std::env::temp_dir().join(format!("concurrent_renames_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    {
        let server = TestServer::start_file_backed(dir.to_str().unwrap()).await.expect("server");
        let pool = server.connect_sqlx().await.expect("connect");
        let mut tables: Vec<(String, bool)> = Vec::new();
        for i in 0..4 {
            pool.execute(AssertSqlSafe(format!("CREATE TABLE pa{i} (id BIGINT PRIMARY KEY, v BIGINT)"))).await.unwrap();
            tables.push((format!("{i}"), false));
        }
        for i in 4..6 {
            pool.execute(AssertSqlSafe(format!(
                "CREATE TABLE pa{i} (id BIGINT NOT NULL PRIMARY KEY, v BIGINT) PARTITION BY HASH (id) PARTITIONS 2"
            )))
            .await
            .unwrap();
            tables.push((format!("{i}"), true));
        }
        for (i, _) in &tables {
            pool.execute(AssertSqlSafe(format!("INSERT INTO pa{i} VALUES (1, {i}), (2, {i})"))).await.unwrap();
        }
        let mut tasks = Vec::new();
        for (i, partitioned) in tables.clone() {
            let pool = pool.clone();
            tasks.push(tokio::spawn(async move {
                for round in 0..ROUNDS {
                    let (from, to) = if round % 2 == 0 { ("pa", "pb") } else { ("pb", "pa") };
                    pool.execute(AssertSqlSafe(format!("ALTER TABLE {from}{i} RENAME TO {to}{i}")))
                        .await
                        .map_err(|e| format!("{from}{i} -> {to}{i} (partitioned {partitioned}, round {round}): {e}"))?;
                }
                Ok::<(), String>(())
            }));
        }
        let mut failures = Vec::new();
        for t in tasks {
            if let Err(e) = t.await.unwrap() {
                failures.push(e);
            }
        }
        assert!(failures.is_empty(), "{failures:?}");
        for (i, _) in &tables {
            let n: i64 = sqlx::query(AssertSqlSafe(format!("SELECT COUNT(*) FROM pa{i} WHERE v = {i}")))
                .fetch_one(&pool)
                .await
                .unwrap_or_else(|e| panic!("pa{i}: {e}"))
                .get(0);
            assert_eq!(n, 2, "pa{i} lost rows");
        }
        let intents: Vec<String> = std::fs::read_dir(&dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .filter(|n| n.contains("rename.intent") || n.contains("rename.partial"))
            .collect();
        assert!(intents.is_empty(), "intents left: {intents:?}");
    }
    let _ = std::fs::remove_dir_all(&dir);
}
