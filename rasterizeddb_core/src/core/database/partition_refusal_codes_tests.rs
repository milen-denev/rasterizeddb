//! Every partition refusal answers a SQLSTATE that names the rule it broke,
//! PostgreSQL's code where PostgreSQL has the same error, and never XX000:
//! a client reads XX000 as the server failing, not as its statement being
//! refused. `CREATE … PARTITION BY`, the conversion of a plain table
//! (`ALTER TABLE … PARTITION BY`), `ALTER` on a partitioned table and the
//! reshape verbs, each refusal run through the executor and its text
//! through [`crate::pgwire::sqlstate_for`], as the wire reports it.
//!
//! The conversion's unique-key and nullable-key refusals answered XX000
//! while `CREATE` answered 0A000 for the same rule: the conversion carried
//! the refusal's text without its kind (`partitioned_refusal_text`).

use std::path::Path;

use rclite::Arc;

use super::Database;

async fn open(dir: &Path) -> Arc<Database> {
    let _ = crate::MAX_PERMITS_THREADS.set(4);
    let _ = crate::BATCH_SIZE.set(512);
    let _ = crate::core::processor::concurrent_processor::ENABLE_CACHE.set(false);
    Arc::new(Database::try_new_named(&dir.to_string_lossy(), false, "refusal_codes").await.expect("open the database"))
}

fn close(dir: &Path, db: Arc<Database>) {
    drop(db);
    crate::core::wal::remove_for(&dir.to_string_lossy());
}

async fn try_exec(db: &Arc<Database>, sql: &str) -> Result<(), String> {
    // `CREATE UNIQUE INDEX` reaches the executor as the constraint the
    // front end rewrites it to.
    let rewritten = crate::core::rql::lexer_alter::unique_index_as_constraint(sql);
    let sql = rewritten.as_deref().unwrap_or(sql);
    let purpose = crate::core::rql::lexer_s1::recognize_query_purpose(sql).ok_or_else(|| format!("parse {sql}"))?;
    let out = crate::core::rql::executor::execute_with_sync(purpose, db.clone(), true).await;
    if out.is_empty() || out[0] == 3 {
        return Err(String::from_utf8_lossy(out.get(1..).unwrap_or_default()).into_owned());
    }
    Ok(())
}

async fn exec(db: &Arc<Database>, sql: &str) {
    try_exec(db, sql).await.unwrap_or_else(|e| panic!("{sql}: {e}"));
}

/// The refusals, each with the code it must answer.
const REFUSALS: &[(&str, &str)] = &[
    // CREATE … PARTITION BY.
    // PostgreSQL: "unique constraint on partitioned table must include all
    // partitioning columns", 0A000.
    ("CREATE TABLE c1 (id BIGINT PRIMARY KEY, k BIGINT NOT NULL) PARTITION BY HASH (k)", "0A000"),
    ("CREATE TABLE c2 (id BIGINT NOT NULL, k BIGINT NOT NULL, UNIQUE (id)) PARTITION BY HASH (k)", "0A000"),
    // PostgreSQL accepts a nullable key; here it is a missing feature.
    ("CREATE TABLE c3 (id BIGINT, k BIGINT) PARTITION BY HASH (k)", "0A000"),
    // PostgreSQL: "cannot use generated column in partition key", 0A000.
    (
        "CREATE TABLE c4 (id BIGINT NOT NULL, k BIGINT NOT NULL GENERATED ALWAYS AS (id * 2) STORED) \
         PARTITION BY HASH (k)",
        "0A000",
    ),
    (
        "CREATE TABLE c5 (a BIGINT NOT NULL, b BIGINT NOT NULL, c BIGINT NOT NULL, d BIGINT NOT NULL) \
         PARTITION BY HASH (a, b, c, d)",
        "0A000",
    ),
    // PostgreSQL: "column \"nosuch\" named in partition key does not exist", 42703.
    ("CREATE TABLE c6 (id BIGINT NOT NULL) PARTITION BY HASH (nosuch)", "42703"),
    // Key types off the §1.4 list; PostgreSQL hashes them.
    ("CREATE TABLE c10 (id BIGINT NOT NULL, f DOUBLE PRECISION NOT NULL) PARTITION BY HASH (f)", "0A000"),
    ("CREATE TABLE c11 (id BIGINT NOT NULL, b BOOLEAN NOT NULL) PARTITION BY HASH (b)", "0A000"),
    ("CREATE TABLE c12 (id BIGINT NOT NULL, j JSON NOT NULL) PARTITION BY HASH (j)", "0A000"),
    ("CREATE TABLE c7 (id BIGINT NOT NULL) PARTITION BY HASH (id) PARTITIONS 0", "22023"),
    ("CREATE TABLE c8 (id BIGINT NOT NULL) PARTITION BY RANGE (id)", "0A000"),
    ("CREATE TABLE c9 (id BIGINT NOT NULL) PARTITION BY LIST (id)", "0A000"),
    // A partitioned table's name taken.
    ("CREATE TABLE pt (id BIGINT NOT NULL) PARTITION BY HASH (id)", "42P07"),
    // Conversion: the same rules as CREATE, and the same codes.
    ("ALTER TABLE pl PARTITION BY HASH (k)", "0A000"),
    ("ALTER TABLE pn PARTITION BY HASH (t)", "0A000"),
    ("ALTER TABLE pl PARTITION BY HASH (nosuch)", "42703"),
    ("ALTER TABLE pl PARTITION BY HASH (id) PARTITIONS 0", "22023"),
    ("ALTER TABLE pf PARTITION BY HASH (f)", "0A000"),
    ("ALTER TABLE pt PARTITION BY HASH (tenant_id)", "0A000"),
    ("ALTER TABLE nosuch PARTITION BY HASH (id)", "42P01"),
    // ALTER on a partitioned table.
    ("ALTER TABLE pt ADD CONSTRAINT u1 UNIQUE (id)", "0A000"),
    ("CREATE UNIQUE INDEX u2 ON pt (id)", "0A000"),
    // PostgreSQL: "cannot drop column … because it is part of the partition
    // key of relation …" and "cannot alter column … because it is part of
    // the partition key …", both 42P16.
    ("ALTER TABLE pt DROP COLUMN tenant_id", "42P16"),
    ("ALTER TABLE pt ALTER COLUMN tenant_id TYPE INT", "42P16"),
    ("ALTER TABLE pt ALTER COLUMN tenant_id DROP NOT NULL", "42P16"),
    // Reshape.
    ("ALTER TABLE pt SPLIT PARTITION FOR (grp = 1)", "22023"),
    ("ALTER TABLE pt SPLIT KEY (tenant_id = 1, grp = 1) INTO 2", "22023"),
    ("ALTER TABLE pl SPLIT PARTITION FOR (id = 1)", "0A000"),
];

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_partition_refusal_answers_its_code() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().to_path_buf();
    let db = open(&dir).await;
    exec(
        &db,
        "CREATE TABLE pt (tenant_id BIGINT NOT NULL, grp BIGINT NOT NULL, id BIGINT NOT NULL, v BIGINT, \
         PRIMARY KEY (tenant_id, grp, id)) PARTITION BY HASH (tenant_id, grp) PARTITIONS 4",
    )
    .await;
    exec(&db, "INSERT INTO pt VALUES (1, 1, 1, 1), (2, 1, 2, 2)").await;
    exec(&db, "CREATE TABLE pl (id BIGINT PRIMARY KEY, k BIGINT NOT NULL)").await;
    exec(&db, "CREATE TABLE pn (id BIGINT PRIMARY KEY, t BIGINT)").await;
    exec(&db, "INSERT INTO pl VALUES (1, 1)").await;
    exec(&db, "INSERT INTO pn VALUES (1, NULL)").await;
    exec(&db, "CREATE TABLE pf (f DOUBLE PRECISION PRIMARY KEY)").await;

    let mut wrong = Vec::new();
    for &(sql, want) in REFUSALS {
        match try_exec(&db, sql).await {
            Ok(()) => wrong.push(format!("{sql}\n    accepted, want {want}")),
            Err(e) => {
                let got = crate::pgwire::sqlstate_for(&e);
                if got != want {
                    wrong.push(format!("{sql}\n    {got}, want {want}: {e}"));
                }
            }
        }
    }
    for t in ["pl", "pn", "pf"] {
        assert!(db.partitioned_table(t).is_none(), "a refusal converted {t}");
    }
    assert!(wrong.is_empty(), "{} of {} refusals answered the wrong code:\n{}", wrong.len(), REFUSALS.len(), wrong.join("\n"));
    close(&dir, db);
}
