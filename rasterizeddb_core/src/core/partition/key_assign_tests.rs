//! A key column assigned the value its row already has (§4.5): accepted for
//! a negative key as for a positive one, and still refused when the value
//! changes (row moves are phase 6's).
//!
//! The scan's `WHERE` reaches the same-value proof as `predicate_rewrite`
//! renders it, which writes a negative literal `- 6`; the tokenizer read
//! that as an operator and a positive number, so no conjunct pinned the key
//! and `ON CONFLICT … DO UPDATE SET tenant_id = EXCLUDED.tenant_id` was
//! refused with 0A000 when the key was negative and existed (the twin, seed
//! 0x5eed2b20, op 1301). The rows after the accepted statements are
//! PostgreSQL 17's after the same statements (`docker run --rm
//! postgres:17`, the script in `.claude/reviews/p5b/pg/accepted.sql`, with
//! NUMERIC there where INT is here: NUMERIC cannot be a partition key).
//! PostgreSQL also accepts the expression forms refused here, and moves the
//! rows whose key changes; neither is built yet (phase 6).

use std::path::Path;

use rclite::Arc;

use crate::core::database::{Database, Relation};

fn path_str(p: &Path) -> String {
    p.to_string_lossy().into_owned()
}

async fn open(dir: &Path) -> Arc<Database> {
    let _ = crate::MAX_PERMITS_THREADS.set(4);
    let _ = crate::BATCH_SIZE.set(512);
    let _ = crate::core::processor::concurrent_processor::ENABLE_CACHE.set(false);
    Arc::new(Database::try_new_named(&path_str(dir), false, "key_assign").await.unwrap())
}

async fn try_exec(db: &Arc<Database>, sql: &str) -> Result<Vec<u8>, String> {
    let purpose = crate::core::rql::lexer_s1::recognize_query_purpose(sql).ok_or_else(|| format!("cannot parse {sql}"))?;
    let out = Box::pin(crate::core::rql::executor::execute_with_sync(purpose, db.clone(), true)).await;
    if out.is_empty() || out[0] == 3 {
        return Err(String::from_utf8_lossy(out.get(1..).unwrap_or_default()).into_owned());
    }
    Ok(out)
}

/// Every row of `table` as its columns' integers (8-byte columns as `i64`,
/// 4-byte as `i32`), sorted.
async fn rows(db: &Arc<Database>, sql: &str) -> Vec<Vec<i64>> {
    let out = try_exec(db, sql).await.unwrap_or_else(|e| panic!("{sql}: {e}"));
    let mut v: Vec<Vec<i64>> = crate::core::row::row::vec_into_rows(&out[1..])
        .unwrap()
        .iter()
        .map(|r| {
            r.columns
                .iter()
                .map(|c| {
                    let b = c.data.into_slice();
                    match b.len() {
                        8 => i64::from_le_bytes(b.try_into().unwrap()),
                        4 => i32::from_le_bytes(b.try_into().unwrap()) as i64,
                        n => panic!("a {n}-byte column"),
                    }
                })
                .collect()
        })
        .collect();
    v.sort();
    v
}

const REFUSED: &str = "changing a partition key value is not supported yet";

/// Mutation: `query_tokenizer::digits_after_sign` not skipping spaces (the
/// tokenizer's behaviour at c3a06643): every accepted statement with a
/// negative key is refused.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_negative_key_assigned_its_own_value_is_accepted_and_a_change_still_refused() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path()).await;
    for ddl in [
        "CREATE TABLE tp (tenant_id BIGINT NOT NULL, grp BIGINT NOT NULL, id BIGINT NOT NULL, n INT, \
         PRIMARY KEY (tenant_id, grp, id)) PARTITION BY HASH (tenant_id) PARTITIONS 7",
        "CREATE TABLE tn (k INT NOT NULL, id BIGINT NOT NULL, n INT, PRIMARY KEY (k, id)) PARTITION BY HASH (k) PARTITIONS 5",
        "CREATE TABLE tc (tenant_id BIGINT NOT NULL, grp BIGINT NOT NULL, id BIGINT NOT NULL, n INT, \
         PRIMARY KEY (tenant_id, grp, id)) PARTITION BY HASH (tenant_id, grp) PARTITIONS 7",
        "INSERT INTO tp (tenant_id, grp, id, n) VALUES (-6, 3, 21, 1), (6, 3, 21, 1)",
        "INSERT INTO tn (k, id, n) VALUES (-65, 1, 1), (65, 1, 1)",
        "INSERT INTO tc (tenant_id, grp, id, n) VALUES (5, -3, 1, 1), (5, 3, 1, 1)",
    ] {
        try_exec(&db, ddl).await.unwrap_or_else(|e| panic!("{ddl}: {e}"));
    }

    // The scan's own rendering of a negative key routes to its partition.
    let tp = match db.relation("tp") {
        Some(Relation::Partitioned(t)) => t,
        _ => panic!("tp is not partitioned"),
    };
    let epoch = tp.epoch().unwrap();
    let tight = tp.route_where("tenant_id = -6").unwrap().pids(&epoch.map);
    assert_eq!(tight.len(), 1);
    assert_eq!(tp.route_where("tenant_id = - 6").unwrap().pids(&epoch.map), tight, "a spaced sign fans out");
    drop((tp, epoch));

    let upsert = "INSERT INTO tp (tenant_id, grp, id, n) VALUES";
    let target = "ON CONFLICT (tenant_id, grp, id) DO UPDATE SET";
    // Accepted, as by PostgreSQL: the key keeps its value.
    for sql in [
        format!("{upsert} (-6, 3, 21, 0) {target} tenant_id = EXCLUDED.tenant_id, n = 14"),
        format!("{upsert} (6, 3, 21, 0) {target} tenant_id = EXCLUDED.tenant_id, n = 14"),
        format!("{upsert} (-6, 3, 21, 0) {target} tenant_id = -6, n = 15"),
        "UPDATE tp SET tenant_id = -6, n = 2 WHERE tenant_id = -6 AND grp = 3".to_string(),
        "UPDATE tp SET tenant_id = -6, n = 3 WHERE tenant_id = - 6".to_string(),
        "UPDATE tn SET k = -65, n = 2 WHERE k = -65".to_string(),
        "INSERT INTO tn (k, id, n) VALUES (-65, 1, 0) ON CONFLICT (k, id) DO UPDATE SET k = EXCLUDED.k, n = 9".to_string(),
        "UPDATE tc SET grp = -3, n = 2 WHERE tenant_id = 5 AND grp = -3".to_string(),
        "INSERT INTO tc (tenant_id, grp, id, n) VALUES (5, -3, 1, 0) ON CONFLICT (tenant_id, grp, id) \
         DO UPDATE SET tenant_id = EXCLUDED.tenant_id, grp = EXCLUDED.grp, n = 9"
            .to_string(),
    ] {
        try_exec(&db, &sql).await.unwrap_or_else(|e| panic!("`{sql}` was refused: {e}"));
    }
    // Refused, before anything is written: the key would change (a sign
    // flip with the same digits included), or an expression computes it,
    // unproven, alike for either sign.
    for sql in [
        format!("{upsert} (-6, 3, 21, 0) {target} tenant_id = -7, n = 18"),
        format!("{upsert} (-6, 3, 21, 0) {target} tenant_id = 6, n = 18"),
        "UPDATE tp SET tenant_id = 6, n = 6 WHERE tenant_id = -6".to_string(),
        "UPDATE tp SET tenant_id = -7 WHERE tenant_id = -6".to_string(),
        "UPDATE tn SET k = 65, n = 2 WHERE k = -65".to_string(),
        "UPDATE tc SET grp = 3, n = 2 WHERE tenant_id = 5 AND grp = -3".to_string(),
        format!("{upsert} (-6, 3, 21, 0) {target} tenant_id = (-6), n = 16"),
        format!("{upsert} (6, 3, 21, 0) {target} tenant_id = (6), n = 16"),
        format!("{upsert} (-6, 3, 21, 0) {target} tenant_id = EXCLUDED.tenant_id + 0, n = 17"),
        format!("{upsert} (6, 3, 21, 0) {target} tenant_id = EXCLUDED.tenant_id + 0, n = 17"),
        "UPDATE tp SET tenant_id = tenant_id, n = 5 WHERE tenant_id = -6".to_string(),
        "UPDATE tp SET tenant_id = tenant_id, n = 5 WHERE tenant_id = 6".to_string(),
    ] {
        match try_exec(&db, &sql).await {
            Err(e) => assert!(e.contains(REFUSED), "`{sql}`: {e}"),
            Ok(_) => panic!("`{sql}` was accepted"),
        }
    }

    // PostgreSQL 17 after the accepted statements; the refused ones wrote
    // nothing.
    assert_eq!(rows(&db, "SELECT tenant_id, grp, id, n FROM tp").await, vec![vec![-6, 3, 21, 3], vec![6, 3, 21, 14]]);
    assert_eq!(rows(&db, "SELECT k, id, n FROM tn").await, vec![vec![-65, 1, 9], vec![65, 1, 1]]);
    assert_eq!(rows(&db, "SELECT tenant_id, grp, id, n FROM tc").await, vec![vec![5, -3, 1, 9], vec![5, 3, 1, 1]]);
    drop(db);
    crate::core::wal::remove_for(&path_str(dir.path()));
}
