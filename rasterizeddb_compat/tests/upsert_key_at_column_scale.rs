//! ON CONFLICT with a key the column stores rounded.
//!
//! `INSERT INTO up VALUES (1.254, 2) ON CONFLICT (k) DO UPDATE …` on a
//! NUMERIC(10,2) key holding 1.25 conflicted, as the key is stored 1.25, so
//! nothing was inserted; the DO UPDATE then looked the row up by `k = 1.254`,
//! matched nothing, and the statement reported success: a lost write. The
//! proposed row is now taken as the column stores it, for the lookup and for
//! `EXCLUDED`, on plain and partitioned tables, for composite keys and an
//! INSERT … SELECT from a wider NUMERIC. The expected text is PostgreSQL 17's.

use rasterizeddb_compat::TestServer;
use tokio_postgres::SimpleQueryMessage;

struct Root(String);

impl Drop for Root {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

async fn text(client: &tokio_postgres::Client, sql: &str) -> Result<Vec<String>, String> {
    let messages = client
        .simple_query(sql)
        .await
        .map_err(|e| e.code().map(|c| c.code().to_string()).unwrap_or_else(|| e.to_string()))?;
    Ok(messages
        .iter()
        .filter_map(|m| match m {
            SimpleQueryMessage::Row(row) => Some(
                (0..row.len()).map(|i| row.get(i).unwrap_or("NULL").to_string()).collect::<Vec<_>>().join("|"),
            ),
            _ => None,
        })
        .collect())
}

async fn upserts(partitioning: &str) {
    let dir = std::env::temp_dir().join(format!("upsert_key_scale_{}_{}", std::process::id(), partitioning.len()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("root");
    let root = Root(dir.to_string_lossy().into_owned());
    let server = TestServer::start_file_backed(&root.0).await.expect("server");
    let (client, connection) = tokio_postgres::connect(&server.url(), tokio_postgres::NoTls).await.expect("connect");
    tokio::spawn(connection);
    let (hash_k, hash_ck, hash_i) = if partitioning.is_empty() {
        (String::new(), String::new(), String::new())
    } else {
        // A NUMERIC cannot be a partition key here, so the NUMERIC-keyed
        // tables are partitioned by the other key column where they have one.
        (
            String::new(),
            " PARTITION BY HASH (t) PARTITIONS 4".to_string(),
            " PARTITION BY HASH (id) PARTITIONS 4".to_string(),
        )
    };
    client
        .batch_execute(&format!(
            "CREATE TABLE up (k NUMERIC(10,2) PRIMARY KEY, v INT){hash_k}; INSERT INTO up VALUES (1.25, 1); \
             CREATE TABLE up2 (k NUMERIC(20,12) PRIMARY KEY, v INT){hash_k}; INSERT INTO up2 VALUES (0.123456789123, 1); \
             CREATE TABLE uc (k NUMERIC(10,2), t INT, v INT, PRIMARY KEY (k, t)){hash_ck}; INSERT INTO uc VALUES (1.25, 7, 1); \
             CREATE TABLE ui (id INT PRIMARY KEY, v INT){hash_i}; INSERT INTO ui VALUES (2, 1); \
             CREATE TABLE s2 (b NUMERIC(38,20)); INSERT INTO s2 VALUES (0.12345678912300000001)"
        ))
        .await
        .expect("setup");
    let mut wrong = Vec::new();
    for (sql, want) in [
        ("INSERT INTO up VALUES (1.254, 2) ON CONFLICT (k) DO UPDATE SET v = 20", &[][..]),
        ("SELECT k, v FROM up", &["1.25|20"]),
        ("INSERT INTO up VALUES (1.2549999999999, 3) ON CONFLICT (k) DO UPDATE SET v = EXCLUDED.v * 10 RETURNING k, v", &["1.25|30"]),
        ("INSERT INTO up VALUES ('1.2500000000001', 5) ON CONFLICT (k) DO UPDATE SET v = v + 1 RETURNING k, v", &["1.25|31"]),
        ("INSERT INTO up VALUES (1.2500000000001, 5) ON CONFLICT (k) DO NOTHING RETURNING k", &[]),
        // `EXCLUDED.k` is the key as stored too.
        ("INSERT INTO up VALUES (1.249, 6) ON CONFLICT (k) DO UPDATE SET v = 1000 + EXCLUDED.v WHERE EXCLUDED.k = 1.25 RETURNING k, v", &["1.25|1006"]),
        ("SELECT k, v FROM up", &["1.25|1006"]),
        ("INSERT INTO up2 VALUES (0.1234567891226, 2) ON CONFLICT (k) DO UPDATE SET v = 20 RETURNING k, v", &["0.123456789123|20"]),
        ("INSERT INTO up2 SELECT b, 3 FROM s2 ON CONFLICT (k) DO UPDATE SET v = 30 RETURNING k, v", &["0.123456789123|30"]),
        ("INSERT INTO up2 SELECT b, 4 FROM s2 ON CONFLICT (k) DO NOTHING RETURNING k", &[]),
        ("SELECT k, v FROM up2", &["0.123456789123|30"]),
        ("INSERT INTO uc VALUES (1.254, 7, 2) ON CONFLICT (k, t) DO UPDATE SET v = 20 RETURNING k, t, v", &["1.25|7|20"]),
        ("SELECT count(*) FROM up", &["1"]),
        ("SELECT count(*) FROM uc", &["1"]),
    ] {
        let got = text(&client, sql).await;
        let want: Vec<String> = want.iter().map(|s| s.to_string()).collect();
        if got.as_ref() != Ok(&want) {
            wrong.push(format!("{sql}\n    got  {got:?}\n    want {want:?}"));
        }
    }
    assert!(wrong.is_empty(), "{partitioning}: {} wrong:\n{}", wrong.len(), wrong.join("\n"));
}

#[tokio::test(flavor = "multi_thread")]
async fn an_upsert_finds_the_row_by_its_key_as_stored_on_a_plain_table() {
    upserts("").await;
}

#[tokio::test(flavor = "multi_thread")]
async fn an_upsert_finds_the_row_by_its_key_as_stored_on_a_hash_partitioned_table() {
    upserts("hash").await;
}
