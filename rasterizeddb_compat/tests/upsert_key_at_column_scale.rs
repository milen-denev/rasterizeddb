//! ON CONFLICT with a key the column stores rounded.
//!
//! `INSERT INTO up VALUES (1.254, 2) ON CONFLICT (k) DO UPDATE …` on a
//! NUMERIC(10,2) key holding 1.25 conflicted, as the key is stored 1.25, so
//! nothing was inserted; the DO UPDATE then looked the row up by `k = 1.254`,
//! matched nothing, and the statement reported success: a lost write. The
//! proposed row is now taken as the column stores it, for the lookup and for
//! `EXCLUDED`, on plain and partitioned tables, for composite keys and an
//! INSERT … SELECT from a wider NUMERIC. The expected text is PostgreSQL 17's.
//!
//! The key is the value as evaluated into the column, never the literal's
//! text: `1.254::numeric`, `CAST('1.254' AS numeric)`, `(1.254)` matched no
//! row; and two rows of one statement whose keys the column stores as one —
//! `(0.995, …), (1.00, …)`, `(5.0, …), (5.00, …)` — were stored twice or
//! applied last-wins, where PostgreSQL raises 21000.

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
             CREATE TABLE s2 (b NUMERIC(38,20)); INSERT INTO s2 VALUES (0.12345678912300000001); \
             CREATE TABLE uu (k UUID PRIMARY KEY, v INT); CREATE TABLE ut (k TIMESTAMP PRIMARY KEY, v INT); \
             CREATE TABLE ub (k BOOLEAN PRIMARY KEY, v INT); CREATE TABLE ux (k TEXT PRIMARY KEY, v INT); \
             CREATE TABLE kn (id INT PRIMARY KEY, u TEXT UNIQUE, v INT); \
             CREATE TABLE kc (x INT, k INT, v INT, UNIQUE (x, k)); \
             CREATE TABLE ks (id SERIAL PRIMARY KEY, v INT); CREATE TABLE kt2 (k TEXT PRIMARY KEY, v INT); \
             CREATE TABLE kf (id INT PRIMARY KEY, k INT UNIQUE DEFAULT 5, v INT); \
             CREATE TABLE ktm (k TIME PRIMARY KEY, v INT); CREATE TABLE kin (k INET PRIMARY KEY, v INT)"
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
        // The key as an expression: evaluated into the column.
        ("INSERT INTO up VALUES (1.254::numeric, 2) ON CONFLICT (k) DO UPDATE SET v = 2 RETURNING k, v", &["1.25|2"]),
        ("INSERT INTO up VALUES ('1.254'::numeric, 3) ON CONFLICT (k) DO UPDATE SET v = 3 RETURNING k, v", &["1.25|3"]),
        ("INSERT INTO up VALUES (CAST('1.254' AS numeric), 4) ON CONFLICT (k) DO UPDATE SET v = 4 RETURNING k, v", &["1.25|4"]),
        ("INSERT INTO up VALUES ((1.254), 5) ON CONFLICT (k) DO UPDATE SET v = 5 RETURNING k, v", &["1.25|5"]),
        ("INSERT INTO up VALUES (1.254::numeric(10,2), 7) ON CONFLICT (k) DO UPDATE SET v = 7 RETURNING k, v", &["1.25|7"]),
        ("INSERT INTO up (v, k) VALUES (9, 1.25 + 0.004) ON CONFLICT (k) DO UPDATE SET v = 9 RETURNING k, v", &["1.25|9"]),
        ("SELECT count(*) FROM up", &["1"]),
        ("SELECT count(*) FROM uc", &["1"]),
    ] {
        let got = text(&client, sql).await;
        let want: Vec<String> = want.iter().map(|s| s.to_string()).collect();
        if got.as_ref() != Ok(&want) {
            wrong.push(format!("{sql}\n    got  {got:?}\n    want {want:?}"));
        }
    }
    // Two rows of one statement that the column stores under one key: 21000,
    // nothing written. As PostgreSQL 17 answers.
    for sql in [
        "INSERT INTO up VALUES (0.995, 3), (1.00, 4) ON CONFLICT (k) DO UPDATE SET v = EXCLUDED.v",
        "INSERT INTO up VALUES (5.0, 1), (5.00, 2) ON CONFLICT (k) DO UPDATE SET v = EXCLUDED.v",
        "INSERT INTO up VALUES (7.10, 1), ('7.1', 2) ON CONFLICT (k) DO UPDATE SET v = EXCLUDED.v",
        "INSERT INTO uc VALUES (2.004, 7, 3), (2.001, 7, 4) ON CONFLICT (k, t) DO UPDATE SET v = EXCLUDED.v",
        "INSERT INTO ui VALUES (5, 1), ('5', 2) ON CONFLICT (id) DO UPDATE SET v = EXCLUDED.v",
        // Every type, not only numbers.
        "INSERT INTO uu VALUES ('A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11', 1), ('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', 2) \
         ON CONFLICT (k) DO UPDATE SET v = EXCLUDED.v",
        "INSERT INTO ut VALUES ('2020-01-01', 1), ('2020-01-01 00:00:00', 2) ON CONFLICT (k) DO UPDATE SET v = EXCLUDED.v",
        "INSERT INTO ub VALUES (true, 1), ('t', 2) ON CONFLICT (k) DO UPDATE SET v = EXCLUDED.v",
        // An omitted key, or DEFAULT, takes a constant default: both rows 5.
        "INSERT INTO kf (id, v) VALUES (1, 1), (2, 2) ON CONFLICT (k) DO UPDATE SET v = EXCLUDED.v",
        "INSERT INTO kf VALUES (1, DEFAULT, 1), (2, 5, 2) ON CONFLICT (k) DO UPDATE SET v = EXCLUDED.v",
        // A key this cannot evaluate is compared as written (as on base):
        // two identical ones are one key.
        "INSERT INTO ktm VALUES ('10:00:00', 1), ('10:00:00', 2) ON CONFLICT (k) DO UPDATE SET v = EXCLUDED.v",
        "INSERT INTO kin VALUES ('10.0.0.1', 1), ('10.0.0.1', 2) ON CONFLICT (k) DO UPDATE SET v = EXCLUDED.v",
        // An integer into a text key is its digits, as the assignment cast.
        "INSERT INTO ux VALUES (6, 1), (6, 2) ON CONFLICT (k) DO UPDATE SET v = EXCLUDED.v",
        "INSERT INTO ux VALUES ('5', 1), (5, 2) ON CONFLICT (k) DO UPDATE SET v = EXCLUDED.v",
    ] {
        let got = text(&client, sql).await;
        if got != Err("21000".to_string()) {
            wrong.push(format!("{sql}\n    got  {got:?}\n    want Err(21000)"));
        }
    }
    for (sql, want) in [
        ("SELECT count(*) FROM up WHERE k IN (1.00, 5.00, 7.10)", &["0"][..]),
        ("SELECT count(*) FROM uc WHERE k = 2.00", &["0"]),
        ("SELECT count(*) FROM ui WHERE id = 5", &["0"]),
        ("SELECT count(*) FROM uu", &["0"]),
        ("SELECT count(*) FROM ut", &["0"]),
        ("SELECT count(*) FROM ub", &["0"]),
        ("SELECT count(*) FROM kf", &["0"]),
        ("SELECT count(*) FROM ktm", &["0"]),
        ("SELECT count(*) FROM kin", &["0"]),
        // NULL equals nothing, and is not the text 'NULL': every row stored,
        // as in PostgreSQL 17.
        ("INSERT INTO kn VALUES (1, NULL, 1), (2, NULL, 2) ON CONFLICT (u) DO UPDATE SET v = EXCLUDED.v", &[]),
        ("INSERT INTO kn VALUES (3, NULL, 1), (4, 'NULL', 2) ON CONFLICT (u) DO UPDATE SET v = EXCLUDED.v", &[]),
        ("SELECT count(*) FROM kn", &["4"]),
        ("INSERT INTO kc VALUES (1, NULL, 1), (1, NULL, 2) ON CONFLICT (x, k) DO UPDATE SET v = EXCLUDED.v", &[]),
        ("SELECT count(*) FROM kc", &["2"]),
        // A sequence's default is a new value per row.
        ("INSERT INTO ks VALUES (DEFAULT, 1), (DEFAULT, 2) ON CONFLICT (id) DO UPDATE SET v = EXCLUDED.v", &[]),
        ("SELECT count(*) FROM ks", &["2"]),
        // Text keys spelled differently are different keys: both stored.
        ("INSERT INTO ux VALUES ('a', 1), ('A', 2), ('a ', 3) ON CONFLICT (k) DO UPDATE SET v = EXCLUDED.v", &[]),
        ("SELECT count(*) FROM ux", &["3"]),
    ] {
        let got = text(&client, sql).await;
        let want: Vec<String> = want.iter().map(|s| s.to_string()).collect();
        if got.as_ref() != Ok(&want) {
            wrong.push(format!("{sql}\n    got  {got:?}\n    want {want:?}"));
        }
    }
    // DEFAULT with no default is NULL, never the text 'DEFAULT': not 21000.
    // (PostgreSQL answers 23502; an explicit DEFAULT into a TEXT key with no
    // default stores '' here, on base as well — left as it is.)
    let sql = "INSERT INTO kt2 VALUES ('DEFAULT', 1), (DEFAULT, 2) ON CONFLICT (k) DO UPDATE SET v = EXCLUDED.v";
    let got = text(&client, sql).await;
    if got == Err("21000".to_string()) {
        wrong.push(format!("{sql}\n    got  {got:?}\n    want anything but Err(21000)"));
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
