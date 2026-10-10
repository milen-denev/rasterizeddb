//! Concurrent upserts against every kind of uniqueness, in every transaction
//! shape: the table must never hold a duplicate key, and `DO UPDATE` must apply
//! exactly once per upsert that reported success.
//!
//! Sixteen clients on their own connections, seeded and bounded (a few
//! hundred statements each over eight keys), hammer the same keys with
//! `ON CONFLICT DO NOTHING` (single-row, multi-row `VALUES`, `INSERT … SELECT`),
//! `ON CONFLICT (k) DO UPDATE SET v = v + 1`, and plain `INSERT` — in
//! autocommit, in explicit transactions that commit, and in ones that roll
//! back. Uniqueness is a single column, a composite, a primary key, and a
//! nullable column (whose NULLs may repeat).
//!
//! Counts are read back as rows, never trusted from the statements' replies:
//! the failure this guards against reports success and stores a duplicate.

use rasterizeddb_compat::TestServer;
use sqlx::{AssertSqlSafe, Connection, Executor, PgConnection, Row};
use std::collections::HashMap;

const CLIENTS: u64 = 16;
const OPS: usize = 60;
const KEYS: i64 = 8;

struct Lcg(u64);
impl Lcg {
    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        self.0 >> 33
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum Mode {
    Auto,
    Commit,
    Rollback,
}

/// What the clients did, for checking against what the table holds.
#[derive(Default)]
struct Outcome {
    /// Committed `DO UPDATE` upserts per key.
    updates: HashMap<String, i64>,
    /// Committed inserts of a NULL key into the nullable table.
    null_inserts: i64,
    /// Statements that succeeded / were refused, by kind, to show the run
    /// actually contended.
    ok: HashMap<&'static str, u64>,
    refused: HashMap<&'static str, u64>,
    /// Anything that was neither success, a uniqueness violation of a plain
    /// INSERT, nor a retryable conflict.
    unexpected: Vec<String>,
}

fn retryable(e: &sqlx::Error) -> bool {
    match e {
        sqlx::Error::Database(d) => matches!(d.code().as_deref(), Some("40001") | Some("40P01")),
        _ => false,
    }
}

fn unique_violation(e: &sqlx::Error) -> bool {
    match e {
        sqlx::Error::Database(d) => d.code().as_deref() == Some("23505"),
        _ => false,
    }
}

/// One statement in the given mode, retried on a retryable conflict. `Ok(true)`
/// means it committed.
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
            Err(e) if retryable(&e) => {
                tokio::time::sleep(std::time::Duration::from_micros(200 * (attempt % 10 + 1))).await;
                continue;
            }
            other => return other,
        }
    }
    Err(sqlx::Error::Protocol("still conflicting after 200 retries".into()))
}

async fn client(url: String, seed: u64) -> Outcome {
    let mut conn = PgConnection::connect(&url).await.expect("connect");
    let mut rng = Lcg(seed);
    let mut out = Outcome::default();
    for _ in 0..OPS {
        let mode = match rng.below(3) {
            0 => Mode::Auto,
            1 => Mode::Commit,
            _ => Mode::Rollback,
        };
        let k = rng.below(KEYS as u64) as i64;
        let k2 = rng.below(KEYS as u64) as i64;
        let k3 = rng.below(KEYS as u64) as i64;
        let (kind, sql, update_key): (&'static str, String, Option<String>) = match rng.below(12) {
            0 => ("single nothing", format!("INSERT INTO s1 (k, v) VALUES ({k}, 1) ON CONFLICT (k) DO NOTHING"), None),
            1 => ("multirow nothing", format!("INSERT INTO s1 (k, v) VALUES ({k}, 1), ({k2}, 1), ({k3}, 1) ON CONFLICT (k) DO NOTHING"), None),
            2 => ("select nothing", format!("INSERT INTO s1 (k, v) SELECT k, 1 FROM src WHERE k = {k} ON CONFLICT (k) DO NOTHING"), None),
            3 => ("plain insert", format!("INSERT INTO s1 (k, v) VALUES ({k}, 1)"), None),
            4 => ("single update", format!("INSERT INTO u1 (k, v) VALUES ({k}, 1) ON CONFLICT (k) DO UPDATE SET v = u1.v + 1"), Some(format!("u1:{k}"))),
            5 => ("composite nothing", format!("INSERT INTO c2 (a, b, v) VALUES ({}, {}, 1) ON CONFLICT (a, b) DO NOTHING", k % 3, k2 % 3), None),
            6 => ("composite update", format!("INSERT INTO cu (a, b, v) VALUES ({}, {}, 1) ON CONFLICT (a, b) DO UPDATE SET v = cu.v + 1", k % 3, k2 % 3), Some(format!("cu:{}:{}", k % 3, k2 % 3))),
            7 => ("pk nothing", format!("INSERT INTO pk (id, v) VALUES ({k}, 1), ({k2}, 1) ON CONFLICT (id) DO NOTHING"), None),
            8 => ("pk update", format!("INSERT INTO pu (id, v) VALUES ({k}, 1) ON CONFLICT (id) DO UPDATE SET v = pu.v + 1"), Some(format!("pu:{k}"))),
            9 => ("pk plain", format!("INSERT INTO pk (id, v) VALUES ({k}, 1)"), None),
            10 => {
                if rng.below(4) == 0 {
                    ("nullable null", "INSERT INTO nu (k, v) VALUES (NULL, 1) ON CONFLICT (k) DO NOTHING".to_string(), Some("nu:NULL".to_string()))
                } else {
                    ("nullable nothing", format!("INSERT INTO nu (k, v) VALUES ({k}, 1) ON CONFLICT (k) DO NOTHING"), None)
                }
            }
            _ => ("nullable plain", format!("INSERT INTO nu (k, v) VALUES ({k}, 1)"), None),
        };
        match run(&mut conn, &sql, mode).await {
            Ok(committed) => {
                *out.ok.entry(kind).or_default() += 1;
                if committed {
                    if let Some(key) = update_key {
                        if key == "nu:NULL" {
                            out.null_inserts += 1;
                        } else {
                            *out.updates.entry(key).or_default() += 1;
                        }
                    }
                }
            }
            Err(e) if unique_violation(&e) && kind.contains("plain") => {
                *out.refused.entry(kind).or_default() += 1;
            }
            Err(e) => {
                let code = match &e {
                    sqlx::Error::Database(d) => d.code().map(|c| c.to_string()).unwrap_or_default(),
                    _ => String::new(),
                };
                out.unexpected.push(format!("{kind} ({mode:?}) [{code}]: {sql}: {e}"))
            }
        }
    }
    out
}

async fn duplicates(pool: &sqlx::PgPool, table: &str, key: &str) -> Vec<String> {
    let mut seen: HashMap<String, usize> = HashMap::new();
    for row in sqlx::raw_sql(AssertSqlSafe(format!("SELECT {key} FROM {table}")))
        .fetch_all(pool)
        .await
        .unwrap_or_else(|e| panic!("read {table}: {e}"))
    {
        let parts: Vec<String> = (0..row.len())
            .map(|i| row.try_get_unchecked::<Option<String>, _>(i).unwrap().unwrap_or_else(|| "NULL".into()))
            .collect();
        *seen.entry(parts.join(":")).or_default() += 1;
    }
    let mut dups: Vec<String> = seen
        .into_iter()
        .filter(|(k, n)| *n > 1 && !k.contains("NULL"))
        .map(|(k, n)| format!("{table} {k} x{n}"))
        .collect();
    dups.sort();
    dups
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_upserts_keep_every_key_unique_and_count_every_update() {
    let server = TestServer::start().await.expect("server should start");
    let pool = server.connect_sqlx().await.expect("should connect");
    for sql in [
        "CREATE TABLE src (k INT NOT NULL)",
        "INSERT INTO src (k) VALUES (0),(1),(2),(3),(4),(5),(6),(7)",
        "CREATE TABLE s1 (k INT NOT NULL UNIQUE, v INT NOT NULL)",
        "CREATE TABLE u1 (k INT NOT NULL UNIQUE, v INT NOT NULL)",
        "CREATE TABLE c2 (a INT NOT NULL, b INT NOT NULL, v INT NOT NULL, UNIQUE (a, b))",
        "CREATE TABLE cu (a INT NOT NULL, b INT NOT NULL, v INT NOT NULL, UNIQUE (a, b))",
        "CREATE TABLE pk (id INT PRIMARY KEY, v INT NOT NULL)",
        "CREATE TABLE pu (id INT PRIMARY KEY, v INT NOT NULL)",
        "CREATE TABLE nu (k INT UNIQUE, v INT NOT NULL)",
    ] {
        pool.execute(AssertSqlSafe(sql)).await.unwrap_or_else(|e| panic!("{sql}: {e}"));
    }

    let mut handles = Vec::new();
    for c in 0..CLIENTS {
        handles.push(tokio::spawn(client(server.url(), 0x5eed_0000 + c * 7919)));
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
        total.null_inserts += o.null_inserts;
        total.unexpected.extend(o.unexpected);
    }
    // What actually happened, so a run without contention cannot pass quietly.
    println!("succeeded: {:?}", total.ok);
    println!("refused as duplicates: {:?}", total.refused);

    let mut problems: Vec<String> = Vec::new();
    for (table, key) in [("s1", "k"), ("u1", "k"), ("c2", "a, b"), ("cu", "a, b"), ("pk", "id"), ("pu", "id"), ("nu", "k")] {
        problems.extend(duplicates(&pool, table, key).await);
    }

    // DO UPDATE: every committed upsert added exactly one.
    for (table, key) in [("u1", "k"), ("cu", "a, b"), ("pu", "id")] {
        let rows = sqlx::raw_sql(AssertSqlSafe(format!("SELECT {key}, v FROM {table}")))
            .fetch_all(&pool)
            .await
            .unwrap();
        let mut stored: HashMap<String, i64> = HashMap::new();
        for row in rows {
            let n = row.len();
            let parts: Vec<String> = (0..n - 1)
                .map(|i| row.try_get_unchecked::<String, _>(i).unwrap())
                .collect();
            let v: String = row.try_get_unchecked(n - 1).unwrap();
            stored.insert(format!("{table}:{}", parts.join(":")), v.parse().unwrap());
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

    // NULLs repeat freely in a nullable UNIQUE column: every committed insert
    // of one is there.
    let nulls: i64 = sqlx::query("SELECT COUNT(*) FROM nu WHERE k IS NULL")
        .fetch_one(&pool)
        .await
        .unwrap()
        .get(0);
    if nulls != total.null_inserts {
        problems.push(format!("nu: {nulls} NULL keys stored, {} committed", total.null_inserts));
    }

    println!("unexpected errors: {}", total.unexpected.len());
    for e in total.unexpected.iter().take(20) {
        println!("  {e}");
    }
    println!("problems: {problems:?}");
    assert!(problems.is_empty(), "{}", problems.join("\n"));
    assert!(total.unexpected.is_empty(), "unexpected errors:\n{}", total.unexpected.join("\n"));
    // The run has to have contended to mean anything.
    assert!(total.refused.values().sum::<u64>() > 10, "almost no conflicts: {:?}", total.refused);
}

/// The composite form of the race `unique_under_concurrency.rs` pins for one
/// column: four connections inserting the same 300 `(a, b)` pairs at once with
/// `ON CONFLICT (a, b) DO NOTHING`. Every pair must be stored exactly once.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_composite_unique_holds_under_concurrent_upserts() {
    const CONNECTIONS: usize = 4;
    const PAIRS: usize = 300;
    let server = TestServer::start().await.expect("server should start");
    let pool = server.connect_sqlx().await.expect("should connect");
    pool.execute("CREATE TABLE cc (a INT NOT NULL, b INT NOT NULL, v INT NOT NULL, UNIQUE (a, b))")
        .await
        .unwrap();
    let values: Vec<String> = (0..PAIRS).map(|i| format!("({}, {}, 1)", i / 20, i % 20)).collect();
    let sql = format!(
        "INSERT INTO cc (a, b, v) VALUES {} ON CONFLICT (a, b) DO NOTHING",
        values.join(",")
    );
    let mut handles = Vec::new();
    for _ in 0..CONNECTIONS {
        let url = server.url();
        let sql = sql.clone();
        handles.push(tokio::spawn(async move {
            let mut conn = PgConnection::connect(&url).await.expect("connect");
            conn.execute(AssertSqlSafe(sql)).await.map(|_| ()).map_err(|e| e.to_string())
        }));
    }
    for h in handles {
        h.await.expect("task").unwrap_or_else(|e| panic!("an upsert failed: {e}"));
    }
    let dups = duplicates(&pool, "cc", "a, b").await;
    let rows: i64 = sqlx::query("SELECT COUNT(*) FROM cc").fetch_one(&pool).await.unwrap().get(0);
    assert!(dups.is_empty() && rows == PAIRS as i64, "{rows} rows for {PAIRS} pairs; duplicated: {dups:?}");
}

/// `INSERT … SELECT … ON CONFLICT DO NOTHING` skips the rows that conflict and
/// writes the rest — single-threaded, so a failure here is not a race. The
/// SELECT is rewritten into a VALUES insert, and the rewrite used to drop the
/// clause: every conflicting row raised a duplicate-key error instead.
#[tokio::test(flavor = "multi_thread")]
async fn insert_select_keeps_its_conflict_clause() {
    let server = TestServer::start().await.expect("server should start");
    let pool = server.connect_sqlx().await.expect("should connect");
    for sql in [
        "CREATE TABLE src (k INT NOT NULL)",
        "INSERT INTO src (k) VALUES (1),(2),(3)",
        "CREATE TABLE dst (k INT NOT NULL UNIQUE, v INT NOT NULL)",
        "INSERT INTO dst (k, v) VALUES (2, 9)",
    ] {
        pool.execute(AssertSqlSafe(sql)).await.unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
    let done = pool
        .execute("INSERT INTO dst (k, v) SELECT k, 1 FROM src ON CONFLICT (k) DO NOTHING")
        .await
        .expect("conflicting rows are skipped, not raised");
    assert_eq!(done.rows_affected(), 2);
    // From a constant SELECT (no FROM) too.
    pool.execute("INSERT INTO dst (k, v) SELECT 3, 7 ON CONFLICT (k) DO NOTHING")
        .await
        .expect("a constant SELECT keeps the clause");
    let rows: Vec<(i32, i32)> = sqlx::query("SELECT k, v FROM dst ORDER BY k")
        .fetch_all(&pool)
        .await
        .unwrap()
        .iter()
        .map(|r| (r.get(0), r.get(1)))
        .collect();
    assert_eq!(rows, vec![(1, 1), (2, 9), (3, 1)]);
    // DO UPDATE through the same rewrite.
    pool.execute("INSERT INTO dst (k, v) SELECT k, 5 FROM src WHERE k = 2 ON CONFLICT (k) DO UPDATE SET v = EXCLUDED.v")
        .await
        .expect("DO UPDATE through INSERT … SELECT");
    let v: i32 = sqlx::query("SELECT v FROM dst WHERE k = 2").fetch_one(&pool).await.unwrap().get(0);
    assert_eq!(v, 5);
}
