//! Point updates, which name their row by a unique key, run side by side
//! rather than one at a time. They take that key's lock stripe instead of the
//! table's write lock, so this pins what the table lock used to guarantee:
//!
//! - no update is lost: `n = n + 1` from many clients on the same few rows
//!   adds exactly one per committed statement;
//! - a row relocated by an update stays visible to the uniqueness check of
//!   its *other* key: inserting a row that reuses an existing email, while
//!   updates by id rewrite the row holding that email, is always refused;
//! - point updates driven by either key, upserts, and rolled-back blocks mix;
//! - two point updates of one row through *different* keys still exclude each
//!   other, or both relocate the same version and the row forks.
//!
//! Counts are read back as rows, never trusted from the statements' replies.

use rasterizeddb_compat::TestServer;
use sqlx::{AssertSqlSafe, Connection, Executor, PgConnection, Row};
use std::collections::HashMap;

const CLIENTS: u64 = 16;
const OPS: usize = 80;
const ROWS: i64 = 6;

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

fn code(e: &sqlx::Error) -> Option<String> {
    match e {
        sqlx::Error::Database(d) => d.code().map(|c| c.to_string()),
        _ => None,
    }
}

/// One statement in the given mode, retried on a retryable conflict.
/// `Ok(true)` means it committed.
async fn run(conn: &mut PgConnection, sql: &str, mode: Mode) -> Result<bool, sqlx::Error> {
    for attempt in 0..300 {
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
    Err(sqlx::Error::Protocol("still conflicting after 300 retries".into()))
}

#[derive(Default)]
struct Outcome {
    /// Committed increments per id.
    increments: HashMap<i64, i64>,
    /// Inserts reusing an existing email that were *accepted*: each is a
    /// duplicate.
    duplicate_inserts: Vec<String>,
    refused: u64,
    unexpected: Vec<String>,
}

async fn client(url: String, index: u64, seed: u64) -> Outcome {
    let mut conn = PgConnection::connect(&url).await.expect("connect");
    let mut rng = Lcg(seed);
    let mut out = Outcome::default();
    for i in 0..OPS {
        let mode = match rng.below(4) {
            0 | 1 => Mode::Auto,
            2 => Mode::Commit,
            _ => Mode::Rollback,
        };
        let id = rng.below(ROWS as u64) as i64;
        let (sql, counts) = match rng.below(4) {
            0 => (format!("UPDATE acct SET n = n + 1 WHERE id = {id}"), true),
            1 => (format!("UPDATE acct SET n = n + 1, note = 'c{index}' WHERE email = 'e{id}'"), true),
            2 => (
                format!(
                    "INSERT INTO acct (id, email, n, note) VALUES ({id}, 'fresh{index}_{i}', 1, '') \
                     ON CONFLICT (id) DO UPDATE SET n = acct.n + 1"
                ),
                true,
            ),
            _ => {
                // Reuses an existing email under a new id: always a duplicate.
                let sql = format!(
                    "INSERT INTO acct (id, email, n, note) VALUES ({}, 'e{id}', 0, '')",
                    1_000_000 + index as i64 * 1000 + i as i64
                );
                match run(&mut conn, &sql, Mode::Auto).await {
                    Ok(_) => out.duplicate_inserts.push(sql),
                    Err(e) if code(&e).as_deref() == Some("23505") => out.refused += 1,
                    Err(e) => out.unexpected.push(format!("{sql}: {e}")),
                }
                continue;
            }
        };
        match run(&mut conn, &sql, mode).await {
            Ok(true) if counts => *out.increments.entry(id).or_default() += 1,
            Ok(_) => {}
            Err(e) => out.unexpected.push(format!("{sql} ({mode:?}): {e}")),
        }
    }
    out
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn point_updates_on_two_unique_keys_lose_nothing_and_admit_no_duplicate() {
    let server = TestServer::start().await.expect("server should start");
    let pool = server.connect_sqlx().await.expect("should connect");
    pool.execute("CREATE TABLE acct (id INT PRIMARY KEY, email TEXT NOT NULL UNIQUE, n INT NOT NULL, note TEXT NOT NULL)")
        .await
        .unwrap();
    for id in 0..ROWS {
        pool.execute(AssertSqlSafe(format!(
            "INSERT INTO acct (id, email, n, note) VALUES ({id}, 'e{id}', 0, '')"
        )))
        .await
        .unwrap();
    }

    let mut handles = Vec::new();
    for c in 0..CLIENTS {
        handles.push(tokio::spawn(client(server.url(), c, 0x9e37_0000 + c * 7919)));
    }
    // Bounded: a forked row makes every later update rewrite each fork, so
    // the versions multiply and the run stalls rather than failing.
    let outcomes = tokio::time::timeout(std::time::Duration::from_secs(120), async {
        let mut outcomes = Vec::new();
        for h in handles {
            outcomes.push(h.await);
        }
        outcomes
    })
    .await
        .expect("the clients stalled: a point update forked a row into several live versions?");
    let mut total = Outcome::default();
    for o in outcomes {
        let o = o.expect("client task");
        for (k, n) in o.increments {
            *total.increments.entry(k).or_default() += n;
        }
        total.duplicate_inserts.extend(o.duplicate_inserts);
        total.refused += o.refused;
        total.unexpected.extend(o.unexpected);
    }
    println!("increments committed: {:?}", total.increments);
    println!("duplicate-email inserts refused: {}", total.refused);

    let rows = sqlx::raw_sql("SELECT id, email, n FROM acct").fetch_all(&pool).await.unwrap();
    let mut stored: HashMap<i64, i64> = HashMap::new();
    let mut emails: HashMap<String, usize> = HashMap::new();
    for row in &rows {
        let id: i64 = row.try_get_unchecked::<String, _>(0).unwrap().parse().unwrap();
        let email: String = row.try_get_unchecked(1).unwrap();
        let n: i64 = row.try_get_unchecked::<String, _>(2).unwrap().parse().unwrap();
        *emails.entry(email).or_default() += 1;
        assert!(stored.insert(id, n).is_none(), "id {id} stored twice");
    }

    let mut problems = Vec::new();
    if rows.len() as i64 != ROWS {
        problems.push(format!("{} rows stored, {ROWS} expected", rows.len()));
    }
    for (email, n) in &emails {
        if *n > 1 {
            problems.push(format!("email {email} stored {n} times"));
        }
    }
    for id in 0..ROWS {
        let want = total.increments.get(&id).copied().unwrap_or(0);
        let got = stored.get(&id).copied().unwrap_or(-1);
        if want != got {
            problems.push(format!("id {id}: n = {got}, {want} increments committed"));
        }
    }
    for sql in total.duplicate_inserts.iter().take(5) {
        problems.push(format!("accepted a duplicate email: {sql}"));
    }
    assert!(problems.is_empty(), "{}", problems.join("\n"));
    assert!(total.unexpected.is_empty(), "unexpected errors:\n{}", total.unexpected.join("\n"));
    // The run must have contended to mean anything.
    assert!(total.refused > 50, "too few duplicate attempts: {}", total.refused);
    assert!(total.increments.values().sum::<i64>() > 200, "too few increments: {:?}", total.increments);
}
