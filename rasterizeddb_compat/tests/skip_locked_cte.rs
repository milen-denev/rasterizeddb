//! Repro harness for the swarm-claim statement reported against the deployed
//! build: a window-function CTE feeding a `FOR UPDATE SKIP LOCKED` CTE that an
//! `UPDATE … FROM` then drives. Against a table with **zero rows** this reached
//! 1955 seconds in production, intermittently.

use rasterizeddb_compat::TestServer;
use sqlx::AssertSqlSafe;
use std::time::{Duration, Instant};

async fn server() -> (TestServer, sqlx::PgPool) {
    let server = TestServer::start().await.expect("server should start");
    let pool = server.connect_sqlx().await.expect("should connect");
    (server, pool)
}

async fn run(pool: &sqlx::PgPool, sql: &str) -> Result<u64, String> {
    sqlx::query(AssertSqlSafe(sql))
        .execute(pool)
        .await
        .map(|r| r.rows_affected())
        .map_err(|e| e.to_string().lines().next().unwrap_or("").to_string())
}

async fn seed(pool: &sqlx::PgPool) {
    run(
        pool,
        "CREATE TABLE vynchat_swarm_task (
            id INT PRIMARY KEY,
            domain_id TEXT NOT NULL,
            status TEXT NOT NULL,
            task_index BIGINT NOT NULL,
            created_at TIMESTAMPTZ NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL
        )",
    )
    .await
    .expect("create");
}

const CLAIM: &str = "WITH runnable AS (
  SELECT id FROM (
    SELECT id, ROW_NUMBER() OVER (PARTITION BY domain_id ORDER BY created_at, task_index) AS domain_rank
    FROM vynchat_swarm_task WHERE status='pending'
  ) ranked WHERE ranked.domain_rank <= 4
),
claimed AS (
  SELECT t.id FROM vynchat_swarm_task t
  WHERE t.id IN (SELECT id FROM runnable)
  ORDER BY t.created_at, t.task_index LIMIT 8 FOR UPDATE SKIP LOCKED
)
UPDATE vynchat_swarm_task AS t SET updated_at='2026-09-01 00:00:00+00'
FROM claimed c WHERE t.id=c.id RETURNING t.id";

/// Ten consecutive runs of the claim statement on an empty table. Each is
/// bounded, so a hang fails the test rather than parking the suite.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn claim_on_an_empty_table_is_fast_every_time() {
    let (_server, pool) = server().await;
    seed(&pool).await;

    let mut slow: Vec<(usize, Duration)> = Vec::new();
    for attempt in 1..=10 {
        let started = Instant::now();
        let outcome = tokio::time::timeout(Duration::from_secs(20), run(&pool, CLAIM)).await;
        let elapsed = started.elapsed();
        match outcome {
            Ok(Ok(n)) => println!("run {attempt}: {elapsed:?} ({n} rows)"),
            Ok(Err(e)) => panic!("run {attempt} failed after {elapsed:?}: {e}"),
            Err(_) => {
                println!("run {attempt}: TIMED OUT after {elapsed:?}");
                slow.push((attempt, elapsed));
                break;
            }
        }
        if elapsed > Duration::from_secs(2) {
            slow.push((attempt, elapsed));
        }
    }
    assert!(slow.is_empty(), "slow runs on an empty table: {slow:?}");
}

/// The same statement with each ingredient removed, to place the cost.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn each_ingredient_alone_is_fast() {
    let (_server, pool) = server().await;
    seed(&pool).await;

    let no_lock = CLAIM.replace(" FOR UPDATE SKIP LOCKED", "");
    let no_window = "WITH claimed AS (
      SELECT t.id FROM vynchat_swarm_task t
      WHERE t.status='pending'
      ORDER BY t.created_at, t.task_index LIMIT 8 FOR UPDATE SKIP LOCKED
    )
    UPDATE vynchat_swarm_task AS t SET updated_at='2026-09-01 00:00:00+00'
    FROM claimed c WHERE t.id=c.id RETURNING t.id";

    for (label, sql) in [("minus SKIP LOCKED", no_lock.as_str()), ("minus window CTE", no_window)] {
        for attempt in 1..=4 {
            let started = Instant::now();
            let outcome = tokio::time::timeout(Duration::from_secs(20), run(&pool, sql)).await;
            let elapsed = started.elapsed();
            match outcome {
                Ok(Ok(_)) => println!("{label} run {attempt}: {elapsed:?}"),
                Ok(Err(e)) => println!("{label} run {attempt}: {elapsed:?} ERROR {e}"),
                Err(_) => panic!("{label} run {attempt}: TIMED OUT after {elapsed:?}"),
            }
        }
    }
}

/// `CURRENT_TIMESTAMP` inside a derived table resolves as a column identifier.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn current_timestamp_resolves_inside_a_subquery() {
    let (_server, pool) = server().await;
    run(
        &pool,
        "CREATE TABLE job (id INT PRIMARY KEY, next_attempt_at TIMESTAMPTZ)",
    )
    .await
    .expect("create");
    run(&pool, "INSERT INTO job (id, next_attempt_at) VALUES (1, NULL)")
        .await
        .expect("insert");

    let rows: Result<Vec<(i32,)>, String> = sqlx::query_as(AssertSqlSafe(
        "SELECT id FROM (
            SELECT id FROM job
            WHERE next_attempt_at IS NULL OR next_attempt_at <= CURRENT_TIMESTAMP
         ) ranked",
    ))
    .fetch_all(&pool)
    .await
    .map_err(|e| e.to_string().lines().next().unwrap_or("").to_string());

    let rows = rows.expect("CURRENT_TIMESTAMP should resolve inside a derived table");
    assert_eq!(rows.len(), 1, "the one row should come back");
}

/// Where exactly does `CURRENT_TIMESTAMP` resolve? Diagnostic, not an
/// assertion: prints the outcome of each shape so the gap can be placed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn current_timestamp_shapes() {
    let (_server, pool) = server().await;
    run(
        &pool,
        "CREATE TABLE job (id INT PRIMARY KEY, next_attempt_at TIMESTAMPTZ)",
    )
    .await
    .expect("create");
    run(&pool, "INSERT INTO job (id, next_attempt_at) VALUES (1, NULL)")
        .await
        .expect("insert");

    for (label, sql) in [
        ("select list", "SELECT CURRENT_TIMESTAMP AS t FROM job"),
        ("select list now()", "SELECT now() AS t FROM job"),
        ("top-level WHERE", "SELECT id FROM job WHERE next_attempt_at IS NULL OR next_attempt_at <= CURRENT_TIMESTAMP"),
        ("top-level WHERE now()", "SELECT id FROM job WHERE next_attempt_at IS NULL OR next_attempt_at <= now()"),
        ("top-level WHERE bare", "SELECT id FROM job WHERE next_attempt_at <= CURRENT_TIMESTAMP"),
        ("derived table", "SELECT id FROM (SELECT id FROM job WHERE next_attempt_at IS NULL OR next_attempt_at <= CURRENT_TIMESTAMP) ranked"),
        ("derived table now()", "SELECT id FROM (SELECT id FROM job WHERE next_attempt_at IS NULL OR next_attempt_at <= now()) ranked"),
        ("cte", "WITH ranked AS (SELECT id FROM job WHERE next_attempt_at <= CURRENT_TIMESTAMP) SELECT id FROM ranked"),
        ("update set", "UPDATE job SET next_attempt_at = CURRENT_TIMESTAMP WHERE id = 1"),
        ("insert value", "INSERT INTO job (id, next_attempt_at) VALUES (2, CURRENT_TIMESTAMP)"),
    ] {
        let outcome = sqlx::query(AssertSqlSafe(sql)).fetch_all(&pool).await;
        match outcome {
            Ok(rows) => println!("  OK   {label}: {} row(s)", rows.len()),
            Err(e) => println!(
                "  FAIL {label}: {}",
                e.to_string().lines().next().unwrap_or("")
            ),
        }
    }
}

/// The keyword constants must compare as the clock, not merely parse. A
/// predicate that resolves and then answers wrongly is the failure mode worth
/// testing for: past rows must match `<= CURRENT_TIMESTAMP` and future ones
/// must not, in a derived table as well as at the top level.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn keyword_constants_compare_as_the_clock() {
    let (_server, pool) = server().await;
    run(
        &pool,
        "CREATE TABLE due (id INT PRIMARY KEY, at TIMESTAMPTZ NOT NULL, on_day DATE NOT NULL)",
    )
    .await
    .expect("create");
    for (id, at, day) in [
        (1, "2000-01-01 00:00:00+00", "2000-01-01"),
        (2, "2099-01-01 00:00:00+00", "2099-01-01"),
    ] {
        run(
            &pool,
            &format!("INSERT INTO due (id, at, on_day) VALUES ({id}, '{at}', '{day}')"),
        )
        .await
        .expect("insert");
    }

    for (label, sql, want) in [
        ("ts past", "SELECT id FROM due WHERE at <= CURRENT_TIMESTAMP", vec![1]),
        ("ts future", "SELECT id FROM due WHERE at > CURRENT_TIMESTAMP", vec![2]),
        ("localtimestamp", "SELECT id FROM due WHERE at <= LOCALTIMESTAMP", vec![1]),
        ("date past", "SELECT id FROM due WHERE on_day < CURRENT_DATE", vec![1]),
        ("date future", "SELECT id FROM due WHERE on_day > CURRENT_DATE", vec![2]),
        (
            "derived table",
            "SELECT id FROM (SELECT id FROM due WHERE at <= CURRENT_TIMESTAMP) d",
            vec![1],
        ),
        (
            "cte",
            "WITH d AS (SELECT id FROM due WHERE at > CURRENT_TIMESTAMP) SELECT id FROM d",
            vec![2],
        ),
    ] {
        let got: Vec<i32> = sqlx::query_as::<_, (i32,)>(AssertSqlSafe(sql))
            .fetch_all(&pool)
            .await
            .unwrap_or_else(|e| panic!("{label}: {sql}: {e}"))
            .into_iter()
            .map(|r| r.0)
            .collect();
        assert_eq!(got, want, "{label}: {sql}");
    }
}

/// A column that carries one of the keyword names — only possible by quoting
/// it, since PostgreSQL reserves the word — must still read its own data
/// rather than the clock.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_quoted_column_named_for_a_keyword_is_still_a_column() {
    let (_server, pool) = server().await;
    run(
        &pool,
        r#"CREATE TABLE odd (id INT PRIMARY KEY, "current_date" INT NOT NULL)"#,
    )
    .await
    .expect("create");
    run(&pool, r#"INSERT INTO odd (id, "current_date") VALUES (1, 7)"#)
        .await
        .expect("insert");

    let rows: Vec<(i32,)> = sqlx::query_as(AssertSqlSafe(
        r#"SELECT id FROM odd WHERE "current_date" = 7"#,
    ))
    .fetch_all(&pool)
    .await
    .expect("a quoted column keeps its own meaning");
    assert_eq!(rows.len(), 1, "the stored 7 should match, not the clock");
}
