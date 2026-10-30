//! A write that reads first, under two kinds of concurrency its statement
//! snapshot must not mishandle.
//!
//! * A `FOR UPDATE SKIP LOCKED` claim inside the write's subquery (the job
//!   queue: `UPDATE workers SET job = (SELECT id FROM jobs … FOR UPDATE SKIP
//!   LOCKED) … RETURNING job`, then the job marked done) claims each job
//!   once. Its locking read shared the statement's snapshot, re-read a row
//!   it had just locked under that same stale snapshot, and handed out jobs
//!   already done: 79 of 300 claimed twice, 276 with a read before it.
//! * Contention on the target alone (a second session incrementing the same
//!   row) replays as a plain write's does: the subqueries read nothing that
//!   changed, so the target is scanned at a fresh snapshot. Scanned under
//!   the subqueries', 91-94% of `… WHERE id IN (SELECT k FROM one)` and all
//!   of its CTE form ended in 40001. No increment is lost either way.

use rasterizeddb_compat::TestServer;
use sqlx::{AssertSqlSafe, Executor, Row};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

const JOBS: i64 = 300;
const WORKERS: i64 = 6;

async fn run(c: &mut sqlx::PgConnection, sql: &str) -> Result<u64, sqlx::Error> {
    c.execute(sqlx::raw_sql(AssertSqlSafe(sql.to_string()))).await.map(|r| r.rows_affected())
}

/// Every job claimed exactly once by `claim` (`{w}` is the worker), each
/// worker in a block: claim, mark the job done, commit.
async fn claims_once(claim: &str, partitioned: bool) {
    struct Root(std::path::PathBuf);
    impl Drop for Root {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    // A partitioned table needs a file-backed database, in a directory only
    // this run names.
    let root = partitioned.then(|| {
        Root(std::env::temp_dir().join(format!("write_subquery_claims_part_{}_{}", std::process::id(), claim.len())))
    });
    let server = match &root {
        Some(root) => {
            let _ = std::fs::remove_dir_all(&root.0);
            std::fs::create_dir_all(&root.0).expect("root");
            TestServer::start_file_backed(&root.0.to_string_lossy()).await.expect("server")
        }
        None => TestServer::start().await.expect("server"),
    };
    let pool = server.connect_sqlx().await.expect("pool");
    let jobs = if partitioned {
        "CREATE TABLE jobs (id INT PRIMARY KEY, state TEXT NOT NULL) PARTITION BY HASH (id) PARTITIONS 4"
    } else {
        "CREATE TABLE jobs (id INT PRIMARY KEY, state TEXT NOT NULL)"
    };
    pool.execute(AssertSqlSafe(format!(
        "{jobs}; CREATE TABLE workers (id INT PRIMARY KEY, job INT); CREATE TABLE pad (id INT PRIMARY KEY)"
    )))
    .await
    .expect("schema");
    let values: Vec<String> = (1..=JOBS).map(|i| format!("({i}, 'new')")).collect();
    pool.execute(AssertSqlSafe(format!("INSERT INTO jobs VALUES {}", values.join(",")))).await.expect("jobs");
    let values: Vec<String> = (1..=WORKERS).map(|i| format!("({i}, NULL)")).collect();
    pool.execute(AssertSqlSafe(format!("INSERT INTO workers VALUES {}", values.join(",")))).await.expect("workers");
    let values: Vec<String> = (1..=500).map(|i| format!("({i})")).collect();
    pool.execute(AssertSqlSafe(format!("INSERT INTO pad VALUES {}", values.join(",")))).await.expect("pad");

    let deadline = Instant::now() + Duration::from_secs(120);
    let mut handles = Vec::new();
    for w in 1..=WORKERS {
        let pool = pool.clone();
        let sql = claim.replace("{w}", &w.to_string());
        handles.push(tokio::spawn(async move {
            let mut c = pool.acquire().await.expect("connection");
            let (mut claimed, mut empty, mut errors) = (Vec::new(), 0, 0u64);
            while empty < 5 {
                assert!(Instant::now() < deadline, "worker {w} did not finish in time");
                run(&mut c, "BEGIN").await.expect("begin");
                let attempt: Result<Option<i32>, sqlx::Error> = async {
                    let rows = c.fetch_all(sqlx::raw_sql(AssertSqlSafe(sql.clone()))).await?;
                    let job: Option<i32> = rows.first().and_then(|r| r.get::<Option<i32>, _>(0));
                    if let Some(j) = job {
                        run(&mut c, &format!("UPDATE jobs SET state = 'done' WHERE id = {j}")).await?;
                    }
                    Ok(job)
                }
                .await;
                match attempt {
                    Ok(job) => {
                        run(&mut c, "COMMIT").await.expect("commit");
                        match job {
                            Some(j) => {
                                claimed.push(j);
                                empty = 0;
                            }
                            None => empty += 1,
                        }
                    }
                    Err(_) => {
                        errors += 1;
                        let _ = run(&mut c, "ROLLBACK").await;
                    }
                }
            }
            (w, claimed, errors)
        }));
    }
    let mut by: BTreeMap<i32, Vec<i64>> = BTreeMap::new();
    let mut errors = 0;
    for h in handles {
        let (w, claimed, e) = h.await.expect("worker");
        errors += e;
        for j in claimed {
            by.entry(j).or_default().push(w);
        }
    }
    let twice: Vec<(&i32, &Vec<i64>)> = by.iter().filter(|(_, ws)| ws.len() > 1).collect();
    let left: i64 = pool.fetch_one("SELECT count(*) FROM jobs WHERE state = 'new'").await.expect("left").get(0);
    assert!(
        twice.is_empty(),
        "{claim} (partitioned {partitioned}): {} jobs claimed twice, e.g. {:?}; {errors} errors",
        twice.len(),
        &twice[..twice.len().min(4)]
    );
    assert_eq!((by.len() as i64, left), (JOBS, 0), "{claim} (partitioned {partitioned}): {errors} errors");
}

const CLAIM: &str =
    "UPDATE workers SET job = (SELECT id FROM jobs WHERE state = 'new' ORDER BY id LIMIT 1 FOR UPDATE SKIP LOCKED) \
     WHERE id = {w} RETURNING job";
/// The same, with a read of another table in the statement before the claim.
const CLAIM_AFTER_A_READ: &str = "UPDATE workers SET job = (SELECT id FROM jobs WHERE state = 'new' \
     AND id > (SELECT min(id) - 1000000 FROM pad) ORDER BY id LIMIT 1 FOR UPDATE SKIP LOCKED) \
     WHERE id = {w} RETURNING job";

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_claim_in_a_writes_subquery_takes_each_job_once() {
    claims_once(CLAIM, false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_claim_after_a_read_takes_each_job_once() {
    claims_once(CLAIM_AFTER_A_READ, false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_claim_from_a_partitioned_queue_takes_each_job_once() {
    claims_once(CLAIM, true).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_claim_after_a_read_from_a_partitioned_queue_takes_each_job_once() {
    claims_once(CLAIM_AFTER_A_READ, true).await;
}

/// `stmt` 200 times against a second session incrementing the same row in
/// a tight loop: at least half succeed (a plain write's rate is all of
/// them; scanned under the subqueries' snapshot, 12-18 and 0 did), and no
/// increment is lost.
async fn replays_like_a_plain_write(stmt: &str, block: bool) {
    let server = TestServer::start().await.expect("server");
    let pool = server.connect_sqlx().await.expect("pool");
    pool.execute(sqlx::raw_sql(
        "CREATE TABLE t (id INT PRIMARY KEY, v INT NOT NULL); CREATE TABLE one (k INT PRIMARY KEY);
         INSERT INTO t VALUES (1, 0); INSERT INTO one VALUES (1);",
    ))
    .await
    .expect("schema");
    let stop = Arc::new(AtomicBool::new(false));
    let other_ok = Arc::new(AtomicU64::new(0));
    let other = {
        let (pool, stop, other_ok) = (pool.clone(), stop.clone(), other_ok.clone());
        tokio::spawn(async move {
            let mut c = pool.acquire().await.expect("connection");
            while !stop.load(Ordering::Relaxed) {
                if let Ok(1) = run(&mut c, "UPDATE t SET v = v + 1 WHERE id = 1").await {
                    other_ok.fetch_add(1, Ordering::Relaxed);
                }
            }
        })
    };
    let mut c = pool.acquire().await.expect("connection");
    let (mut ok, mut failed) = (0u64, 0u64);
    let mut first = None;
    let deadline = Instant::now() + Duration::from_secs(120);
    for _ in 0..200 {
        assert!(Instant::now() < deadline, "{stmt}: did not finish in time");
        if block {
            run(&mut c, "BEGIN ISOLATION LEVEL READ COMMITTED").await.expect("begin");
        }
        let r = run(&mut c, stmt).await;
        let r = match (r, block) {
            (Ok(n), true) => run(&mut c, "COMMIT").await.map(|_| n),
            (Err(e), true) => {
                let _ = run(&mut c, "ROLLBACK").await;
                Err(e)
            }
            (r, false) => r,
        };
        match r {
            Ok(1) => ok += 1,
            Ok(n) => panic!("{stmt}: touched {n} rows"),
            Err(e) => {
                failed += 1;
                first.get_or_insert(e.to_string());
            }
        }
    }
    stop.store(true, Ordering::Relaxed);
    other.await.expect("incrementer");
    let v: i32 = pool.fetch_one("SELECT v FROM t WHERE id = 1").await.expect("v").get(0);
    let other_ok = other_ok.load(Ordering::Relaxed);
    assert_eq!(v as u64, ok + other_ok, "{stmt} (block {block}): an increment was lost or doubled");
    assert!(other_ok > 0, "{stmt}: the incrementer never ran");
    assert!(ok >= 100, "{stmt} (block {block}): {ok} of 200 succeeded, {failed} failed; first {first:?}");
}

const CONFLICT_FORMS: [&str; 4] = [
    "UPDATE t SET v = v + 1 WHERE id IN (SELECT k FROM one)",
    "UPDATE t SET v = v + 1 WHERE EXISTS (SELECT 1 FROM one WHERE one.k = t.id)",
    "WITH c AS (SELECT k FROM one) UPDATE t SET v = v + 1 WHERE id IN (SELECT k FROM c)",
    "UPDATE t SET v = t.v + 1 FROM (SELECT k FROM one) d WHERE t.id = d.k",
];

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn target_contention_replays_like_a_plain_write_autocommit() {
    for stmt in CONFLICT_FORMS {
        replays_like_a_plain_write(stmt, false).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn target_contention_replays_like_a_plain_write_read_committed() {
    for stmt in CONFLICT_FORMS {
        replays_like_a_plain_write(stmt, true).await;
    }
}
