//! The partitioned twin of `concurrent_upsert_matrix.rs` (partitioning S18,
//! I10): concurrent upserts against a partitioned table's unique keys, in
//! every transaction shape. The table must never hold a duplicate key, and
//! `DO UPDATE` must apply exactly once per upsert that reported success.
//!
//! Every table is `PARTITION BY HASH (tenant_id, grp) PARTITIONS 7`, with a
//! composite primary key and a composite UNIQUE, each holding the key (as
//! every unique key of a partitioned table must). Uniqueness is decided in
//! the routed partition, by the same in-flight-writer rule a plain table
//! uses; this is what shows that rule holds there.
//!
//! Sixteen clients on their own connections, seeded and bounded (sixty
//! statements each over 24 keys spread across the partitions), hammer the
//! same keys with `ON CONFLICT DO NOTHING` (single-row, multi-row `VALUES`
//! over several partitions, `INSERT … SELECT`), `ON CONFLICT … DO UPDATE SET
//! n = n + 1` (single-row and multi-row), and plain `INSERT`, in autocommit,
//! in blocks that commit, and in blocks that roll back.
//!
//! Counts are read back as rows, never trusted from the statements' replies:
//! the failure this guards against reports success and stores a duplicate.

use rasterizeddb_compat::TestServer;
use sqlx::{AssertSqlSafe, Connection, Executor, PgConnection, Row};
use std::collections::HashMap;

const CLIENTS: u64 = 16;
const OPS: usize = 60;
const KEYS: u64 = 24;
const COLUMNS: &str = "(tenant_id BIGINT NOT NULL, grp BIGINT NOT NULL, id BIGINT NOT NULL, email TEXT, n INT, \
                       PRIMARY KEY (tenant_id, grp, id), UNIQUE (tenant_id, grp, email))";
const PARTITIONING: &str = "PARTITION BY HASH (tenant_id, grp) PARTITIONS 7";

struct Lcg(u64);
impl Lcg {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        self.0 >> 33
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

/// Key `k`'s `(tenant_id, grp, id)`: six tenants by two groups (twelve
/// partition-key values over seven partitions), two ids each.
fn key(k: u64) -> (u64, u64, u64) {
    (k % 6 + 1, k / 6 % 2 + 1, k / 12 + 1)
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum Mode {
    Auto,
    Commit,
    Rollback,
}

#[derive(Default)]
struct Outcome {
    /// Committed `DO UPDATE` upserts per key (`table:tenant:grp:id-or-email`).
    updates: HashMap<String, i64>,
    ok: HashMap<&'static str, u64>,
    refused: HashMap<&'static str, u64>,
    unexpected: Vec<String>,
}

fn code(e: &sqlx::Error) -> Option<String> {
    match e {
        sqlx::Error::Database(d) => d.code().map(|c| c.to_string()),
        _ => None,
    }
}

/// One statement in the given mode, retried on a retryable conflict.
/// `Ok(true)` means it committed.
async fn run(conn: &mut PgConnection, sql: &str, mode: Mode) -> Result<bool, sqlx::Error> {
    for attempt in 0..200 {
        let result = match mode {
            Mode::Auto => conn.execute(AssertSqlSafe(sql.to_string())).await.map(|_| true),
            Mode::Commit | Mode::Rollback => {
                let r = async {
                    conn.execute("BEGIN").await?;
                    conn.execute(AssertSqlSafe(sql.to_string())).await?;
                    if mode == Mode::Commit {
                        conn.execute("COMMIT").await?;
                        Ok(true)
                    } else {
                        conn.execute("ROLLBACK").await?;
                        Ok(false)
                    }
                }
                .await;
                if r.is_err() {
                    let _ = conn.execute("ROLLBACK").await;
                }
                r
            }
        };
        match result {
            Err(e) if matches!(code(&e).as_deref(), Some("40001") | Some("40P01")) => {
                tokio::time::sleep(std::time::Duration::from_micros(200 * (attempt % 10 + 1))).await;
                continue;
            }
            other => return other,
        }
    }
    Err(sqlx::Error::Protocol("still conflicting after 200 retries".into()))
}

async fn client(url: String, seed: u64, client_no: u64) -> Outcome {
    let mut conn = PgConnection::connect(&url).await.expect("connect");
    let mut rng = Lcg(seed);
    let mut out = Outcome::default();
    for op in 0..OPS {
        let mode = match rng.below(3) {
            0 => Mode::Auto,
            1 => Mode::Commit,
            _ => Mode::Rollback,
        };
        let (t, g, i) = key(rng.below(KEYS));
        let (t2, g2, i2) = key(rng.below(KEYS));
        let (t3, g3, i3) = key(rng.below(KEYS));
        // A fresh id, never stored: on the email tables the primary key never
        // conflicts, so the composite UNIQUE is the one deciding.
        let fresh = 1_000_000 + client_no * 1000 + op as u64;
        let (kind, sql, update_keys): (&'static str, String, Vec<String>) = match rng.below(10) {
            0 => ("pk nothing", format!("INSERT INTO pn (tenant_id, grp, id, n) VALUES ({t}, {g}, {i}, 1) ON CONFLICT (tenant_id, grp, id) DO NOTHING"), vec![]),
            1 => (
                "pk multirow nothing",
                format!(
                    "INSERT INTO pn (tenant_id, grp, id, n) VALUES ({t}, {g}, {i}, 1), ({t2}, {g2}, {i2}, 1), ({t3}, {g3}, {i3}, 1) \
                     ON CONFLICT DO NOTHING"
                ),
                vec![],
            ),
            2 => (
                "pk select nothing",
                format!(
                    "INSERT INTO pn (tenant_id, grp, id, n) SELECT tenant_id, grp, id, 1 FROM src \
                     WHERE tenant_id = {t} AND grp = {g} ON CONFLICT (id, grp, tenant_id) DO NOTHING"
                ),
                vec![],
            ),
            3 => ("pk plain", format!("INSERT INTO pn (tenant_id, grp, id, n) VALUES ({t}, {g}, {i}, 1)"), vec![]),
            4 => (
                "pk update",
                format!("INSERT INTO pu (tenant_id, grp, id, n) VALUES ({t}, {g}, {i}, 1) ON CONFLICT (tenant_id, grp, id) DO UPDATE SET n = pu.n + 1"),
                vec![format!("pu:{t}:{g}:{i}")],
            ),
            5 => {
                // Two distinct keys, usually in two partitions (PostgreSQL
                // refuses a row affected twice by one DO UPDATE).
                if (t, g, i) == (t2, g2, i2) {
                    (
                        "pk update by name",
                        format!("INSERT INTO pu (tenant_id, grp, id, n) VALUES ({t}, {g}, {i}, 1) ON CONFLICT ON CONSTRAINT pu_pkey DO UPDATE SET n = pu.n + 1"),
                        vec![format!("pu:{t}:{g}:{i}")],
                    )
                } else {
                    (
                        "pk multirow update",
                        format!(
                            "INSERT INTO pu (tenant_id, grp, id, n) VALUES ({t}, {g}, {i}, 1), ({t2}, {g2}, {i2}, 1) \
                             ON CONFLICT (tenant_id, grp, id) DO UPDATE SET n = pu.n + 1"
                        ),
                        vec![format!("pu:{t}:{g}:{i}"), format!("pu:{t2}:{g2}:{i2}")],
                    )
                }
            }
            6 => (
                "email nothing",
                format!("INSERT INTO en (tenant_id, grp, id, email, n) VALUES ({t}, {g}, {fresh}, 'e{i}', 1) ON CONFLICT (tenant_id, grp, email) DO NOTHING"),
                vec![],
            ),
            7 => ("email plain", format!("INSERT INTO en (tenant_id, grp, id, email, n) VALUES ({t}, {g}, {fresh}, 'e{i}', 1)"), vec![]),
            8 => (
                "email update",
                format!(
                    "INSERT INTO eu (tenant_id, grp, id, email, n) VALUES ({t}, {g}, {fresh}, 'e{i}', 1) \
                     ON CONFLICT (tenant_id, grp, email) DO UPDATE SET n = eu.n + 1"
                ),
                vec![format!("eu:{t}:{g}:e{i}")],
            ),
            _ => (
                "email update returning",
                format!(
                    "INSERT INTO eu (tenant_id, grp, id, email, n) VALUES ({t}, {g}, {fresh}, 'e{i}', 1) \
                     ON CONFLICT (tenant_id, grp, email) DO UPDATE SET n = eu.n + 1 RETURNING n"
                ),
                vec![format!("eu:{t}:{g}:e{i}")],
            ),
        };
        match run(&mut conn, &sql, mode).await {
            Ok(committed) => {
                *out.ok.entry(kind).or_default() += 1;
                if committed {
                    for k in update_keys {
                        *out.updates.entry(k).or_default() += 1;
                    }
                }
            }
            Err(e) if code(&e).as_deref() == Some("23505") && kind.contains("plain") => {
                *out.refused.entry(kind).or_default() += 1;
            }
            Err(e) => out.unexpected.push(format!("{kind} ({mode:?}) [{:?}]: {sql}: {e}", code(&e))),
        }
    }
    out
}

/// Values of `columns` stored more than once in `table` (NULLs repeat
/// freely in a UNIQUE, so a value holding one is not counted).
async fn duplicates(pool: &sqlx::PgPool, table: &str, columns: &str) -> Vec<String> {
    let mut seen: HashMap<String, usize> = HashMap::new();
    for row in sqlx::raw_sql(AssertSqlSafe(format!("SELECT {columns} FROM {table}")))
        .fetch_all(pool)
        .await
        .unwrap_or_else(|e| panic!("read {table}: {e}"))
    {
        let parts: Vec<String> = (0..row.len())
            .map(|i| row.try_get_unchecked::<Option<String>, _>(i).unwrap().unwrap_or_else(|| "NULL".into()))
            .collect();
        *seen.entry(parts.join(":")).or_default() += 1;
    }
    let mut dups: Vec<String> =
        seen.into_iter().filter(|(k, n)| *n > 1 && !k.contains("NULL")).map(|(k, n)| format!("{table} {k} x{n}")).collect();
    dups.sort();
    dups
}

/// A file-backed server's root, removed on drop (after the server, which is
/// declared after it). Partitioned tables need a file-backed database.
struct Root(String);

impl Drop for Root {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn root(tag: &str) -> Root {
    let r = std::env::temp_dir().join(format!("partition_upsert_matrix_{}_{tag}", std::process::id()));
    let _ = std::fs::remove_dir_all(&r);
    std::fs::create_dir_all(&r).expect("root");
    Root(r.to_string_lossy().into_owned())
}

async fn create(pool: &sqlx::PgPool, name: &str) {
    let sql = format!("CREATE TABLE {name} {COLUMNS} {PARTITIONING}");
    pool.execute(AssertSqlSafe(sql.clone())).await.unwrap_or_else(|e| panic!("{sql}: {e}"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_upserts_on_a_partitioned_table_keep_every_key_unique_and_count_every_update() {
    let started = std::time::Instant::now();
    let dir = root("matrix");
    let server = TestServer::start_file_backed(&dir.0).await.expect("server should start");
    let pool = server.connect_sqlx().await.expect("should connect");
    for name in ["pn", "pu", "en", "eu"] {
        create(&pool, name).await;
    }
    pool.execute("CREATE TABLE src (tenant_id BIGINT NOT NULL, grp BIGINT NOT NULL, id BIGINT NOT NULL)")
        .await
        .unwrap();
    let src: Vec<String> = (0..KEYS).map(key).map(|(t, g, i)| format!("({t}, {g}, {i})")).collect();
    pool.execute(AssertSqlSafe(format!("INSERT INTO src (tenant_id, grp, id) VALUES {}", src.join(", "))))
        .await
        .unwrap();

    let mut handles = Vec::new();
    for c in 0..CLIENTS {
        handles.push(tokio::spawn(client(server.url(), 0x5eed_9000 + c * 7919, c)));
    }
    let mut total = Outcome::default();
    for h in handles {
        let o = h.await.expect("client task");
        for (k, n) in o.updates {
            *total.updates.entry(k).or_default() += n;
        }
        for (k, n) in o.ok {
            *total.ok.entry(k).or_default() += n;
        }
        for (k, n) in o.refused {
            *total.refused.entry(k).or_default() += n;
        }
        total.unexpected.extend(o.unexpected);
    }
    println!("succeeded: {:?}", total.ok);
    println!("refused as duplicates: {:?}", total.refused);

    let mut problems: Vec<String> = Vec::new();
    for (table, columns) in [
        ("pn", "tenant_id, grp, id"),
        ("pu", "tenant_id, grp, id"),
        ("en", "tenant_id, grp, email"),
        ("eu", "tenant_id, grp, email"),
        ("en", "tenant_id, grp, id"),
    ] {
        problems.extend(duplicates(&pool, table, columns).await);
    }

    // DO UPDATE: every committed upsert added exactly one (the insert is the
    // first), so the stored n is the number of committed upserts of that key.
    for (table, columns) in [("pu", "tenant_id, grp, id"), ("eu", "tenant_id, grp, email")] {
        let rows = sqlx::raw_sql(AssertSqlSafe(format!("SELECT {columns}, n FROM {table}"))).fetch_all(&pool).await.unwrap();
        let mut stored: HashMap<String, i64> = HashMap::new();
        for row in rows {
            let parts: Vec<String> = (0..3).map(|i| row.try_get_unchecked::<String, _>(i).unwrap()).collect();
            let n: String = row.try_get_unchecked(3).unwrap();
            *stored.entry(format!("{table}:{}", parts.join(":"))).or_default() += n.parse::<i64>().unwrap();
        }
        let expected: HashMap<String, i64> = total
            .updates
            .iter()
            .filter(|(k, _)| k.starts_with(&format!("{table}:")))
            .map(|(k, n)| (k.clone(), *n))
            .collect();
        if stored != expected {
            problems.push(format!("{table}: stored {stored:?}, committed upserts {expected:?}"));
        }
    }

    println!("unexpected errors: {}", total.unexpected.len());
    for e in total.unexpected.iter().take(20) {
        println!("  {e}");
    }
    assert!(problems.is_empty(), "{}", problems.join("\n"));
    assert!(total.unexpected.is_empty(), "unexpected errors:\n{}", total.unexpected.join("\n"));
    // The run has to have contended to mean anything.
    assert!(total.refused.values().sum::<u64>() > 10, "almost no conflicts: {:?}", total.refused);
    assert!(started.elapsed() < std::time::Duration::from_secs(120), "over the cap: {:?}", started.elapsed());
}

/// The composite race `concurrent_upsert_matrix.rs` pins for a plain table,
/// on a partitioned one: four connections inserting the same 300 keys,
/// spread over every partition, at once with `ON CONFLICT DO NOTHING`, by the
/// primary key on one table and by the composite UNIQUE on another. Every key
/// must be stored exactly once.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn three_hundred_keys_across_partitions_are_stored_once_under_concurrent_upserts() {
    const CONNECTIONS: usize = 4;
    const ROWS: u64 = 300;
    let dir = root("three_hundred");
    let server = TestServer::start_file_backed(&dir.0).await.expect("server should start");
    let pool = server.connect_sqlx().await.expect("should connect");
    create(&pool, "cp").await;
    create(&pool, "ce").await;
    // (tenant 1..=10, grp 1..=3, id 0..10): thirty partition-key values.
    let values = |id_of: &dyn Fn(u64) -> u64| -> Vec<String> {
        (0..ROWS).map(|i| format!("({}, {}, {}, 'm{i}', 1)", i % 10 + 1, i / 10 % 3 + 1, id_of(i))).collect()
    };
    let mut statements = vec![format!(
        "INSERT INTO cp (tenant_id, grp, id, email, n) VALUES {} ON CONFLICT (tenant_id, grp, id) DO NOTHING",
        values(&|i| i / 30).join(",")
    )];
    // On `ce` each connection writes its own ids, so only the email decides.
    for c in 0..CONNECTIONS as u64 {
        statements.push(format!(
            "INSERT INTO ce (tenant_id, grp, id, email, n) VALUES {} ON CONFLICT (tenant_id, grp, email) DO NOTHING",
            values(&|i| c * 1000 + i).join(",")
        ));
    }
    let mut handles = Vec::new();
    for c in 0..CONNECTIONS {
        for sql in [statements[0].clone(), statements[c + 1].clone()] {
            let url = server.url();
            handles.push(tokio::spawn(async move {
                let mut conn = PgConnection::connect(&url).await.expect("connect");
                conn.execute(AssertSqlSafe(sql)).await.map(|_| ()).map_err(|e| e.to_string())
            }));
        }
    }
    for h in handles {
        h.await.expect("task").unwrap_or_else(|e| panic!("an upsert failed: {e}"));
    }
    for (table, columns) in [("cp", "tenant_id, grp, id"), ("ce", "tenant_id, grp, email")] {
        let dups = duplicates(&pool, table, columns).await;
        let rows: i64 = sqlx::query(AssertSqlSafe(format!("SELECT COUNT(*) FROM {table}")))
            .fetch_one(&pool)
            .await
            .unwrap()
            .get(0);
        assert!(dups.is_empty() && rows == ROWS as i64, "{table}: {rows} rows for {ROWS} keys; duplicated: {dups:?}");
    }
}
