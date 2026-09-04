//! `SHOW QUERIES`, `SHOW QUERY STATS` and `pg_stat_statements`, as a driver
//! reads them.
//!
//! The whole surface is one test rather than several: the statement log is
//! process-wide, so two tests reading it back would see each other's statements
//! and `RESET QUERY STATS` in one would empty the other's. The phases below run
//! in order for that reason.
//!
//! The column names are written out here rather than read from the engine's own
//! list, because half of them are fixed by PostgreSQL and an independent
//! spelling is the only oracle that catches both sides being wrong together.

use rasterizeddb_compat::TestServer;
use sqlx::AssertSqlSafe;
use sqlx::{Column, Executor, Row};

async fn run(pool: &sqlx::PgPool, sql: &str) -> Result<(), String> {
    pool.execute(sqlx::raw_sql(AssertSqlSafe(sql)))
        .await
        .map(|_| ())
        .map_err(|e| e.to_string().lines().next().unwrap_or("").to_string())
}

async fn rows(pool: &sqlx::PgPool, sql: &str) -> Vec<sqlx::postgres::PgRow> {
    sqlx::query(AssertSqlSafe(sql))
        .fetch_all(pool)
        .await
        .unwrap_or_else(|e| panic!("{} should succeed: {}", sql, e))
}

#[tokio::test(flavor = "multi_thread")]
async fn the_statement_log_reports_what_ran_and_what_it_read() {
    let server = TestServer::start().await.expect("server should start");
    let pool = server.connect_sqlx().await.expect("should connect");

    run(&pool, "CREATE TABLE orders (id INT PRIMARY KEY, amount INT NOT NULL)")
        .await
        .expect("create");
    for id in 1..=50 {
        run(
            &pool,
            &format!("INSERT INTO orders (id, amount) VALUES ({}, {})", id, id),
        )
        .await
        .expect("insert");
    }

    // Everything above is setup noise. From here the log is this test's.
    run(&pool, "RESET QUERY STATS").await.expect("reset");

    // --- what one statement cost -------------------------------------------
    let selected = rows(&pool, "SELECT id FROM orders WHERE amount > 40").await;
    assert_eq!(selected.len(), 10, "ten of the fifty rows match");

    let recent = rows(&pool, "SHOW QUERIES").await;
    assert_eq!(
        recent[0]
            .columns()
            .iter()
            .map(|c| c.name().to_string())
            .collect::<Vec<_>>(),
        vec![
            "started_at",
            "duration_ms",
            "rows",
            "rows_scanned",
            "materialised",
            "materialised_ms",
            "status",
            "database",
            "username",
            "statement",
            "error",
        ],
    );

    let mine = recent
        .iter()
        .find(|r| r.get::<String, _>("statement").contains("WHERE amount > 40"))
        .expect("the statement that just ran is in the log");
    assert_eq!(mine.get::<i64, _>("rows"), 10, "rows returned");
    assert_eq!(
        mine.get::<i64, _>("rows_scanned"),
        50,
        "every row was a candidate: nothing indexes `amount`"
    );
    assert_eq!(mine.get::<String, _>("status"), "ok");
    assert!(mine.get::<String, _>("error").is_empty());
    assert!(
        mine.get::<f64, _>("duration_ms") >= 0.0,
        "a duration is reported"
    );
    assert!(!mine.get::<String, _>("started_at").is_empty());

    // --- a statement that failed is in the log too, as a failure -----------
    let refused = run(&pool, "SELECT * FROM no_such_table").await;
    assert!(refused.is_err(), "the table does not exist");

    let failed = rows(&pool, "SHOW QUERIES")
        .await
        .into_iter()
        .find(|r| r.get::<String, _>("statement").contains("no_such_table"))
        .expect("a refused statement is still a statement that ran");
    assert_eq!(failed.get::<String, _>("status"), "error");
    assert!(
        !failed.get::<String, _>("error").is_empty(),
        "the reason is kept, not just the fact"
    );

    // --- the same statement with a different literal is one shape ----------
    for amount in [10, 20, 30] {
        rows(&pool, &format!("SELECT id FROM orders WHERE amount > {}", amount)).await;
    }

    let stats = rows(&pool, "SHOW QUERY STATS").await;
    assert_eq!(
        stats[0]
            .columns()
            .iter()
            .map(|c| c.name().to_string())
            .collect::<Vec<_>>(),
        vec![
            "queryid",
            "calls",
            "total_ms",
            "mean_ms",
            "min_ms",
            "max_ms",
            "stddev_ms",
            "rows",
            "rows_scanned",
            "materialised",
            "materialised_ms",
            "errors",
            "last_seen",
            "statement",
        ],
    );

    let shape = stats
        .iter()
        .find(|r| r.get::<String, _>("statement") == "SELECT id FROM orders WHERE amount > $1")
        .expect("four executions with four literals are one shape");
    assert_eq!(shape.get::<i64, _>("calls"), 4);
    assert_eq!(
        shape.get::<i64, _>("rows_scanned"),
        200,
        "fifty candidate rows per execution"
    );
    assert!(shape.get::<f64, _>("mean_ms") >= 0.0);

    // --- and the same aggregate under PostgreSQL's own name ----------------
    let pg = rows(
        &pool,
        "SELECT queryid, query, calls, rows, rows_scanned, total_exec_time, mean_exec_time \
         FROM pg_stat_statements",
    )
    .await;
    let pg_shape = pg
        .iter()
        .find(|r| r.get::<String, _>("query") == "SELECT id FROM orders WHERE amount > $1")
        .expect("pg_stat_statements holds the same shapes");
    assert_eq!(pg_shape.get::<i64, _>("calls"), 4);
    assert_eq!(pg_shape.get::<i64, _>("rows_scanned"), 200);
    assert_eq!(
        pg_shape.get::<i64, _>("queryid"),
        shape.get::<i64, _>("queryid"),
        "one statement has one id whichever surface reports it"
    );
    assert!(
        pg_shape.get::<f64, _>("total_exec_time") >= pg_shape.get::<f64, _>("mean_exec_time"),
        "four calls total at least what one of them averaged"
    );

    // A driver asking what the view returns before running it must be told,
    // which is the failure that made every other catalogue view unreadable by
    // column name.
    assert_eq!(
        rows(&pool, "SELECT * FROM pg_stat_statements")
            .await
            .first()
            .expect("at least one shape")
            .columns()
            .iter()
            .map(|c| c.name().to_string())
            .collect::<Vec<_>>(),
        vec![
            "userid",
            "dbid",
            "toplevel",
            "queryid",
            "query",
            "calls",
            "total_exec_time",
            "min_exec_time",
            "max_exec_time",
            "mean_exec_time",
            "stddev_exec_time",
            "rows",
            "shared_blks_hit",
            "shared_blks_read",
            "rows_scanned",
            "materialised",
            "materialised_ms",
        ],
    );

    // --- forgetting ---------------------------------------------------------
    run(&pool, "RESET QUERY STATS").await.expect("reset");
    let after = rows(&pool, "SHOW QUERIES").await;
    assert!(
        after
            .iter()
            .all(|r| !r.get::<String, _>("statement").contains("orders")),
        "nothing from before the reset survives it"
    );

    // --- a password is never what the log keeps -----------------------------
    //
    // `CREATE ROLE … PASSWORD '…'` is readable by any session that can run
    // `SHOW QUERIES`, so the secret is cut out before the record is stored
    // rather than before it is printed.
    run(&pool, "CREATE USER logtest PASSWORD 'hunter2'")
        .await
        .expect("create role");

    let statements: Vec<String> = rows(&pool, "SHOW QUERIES")
        .await
        .iter()
        .map(|r| r.get::<String, _>("statement"))
        .collect();
    assert!(
        statements.iter().any(|s| s.contains("CREATE USER logtest")),
        "the statement is recorded: {:?}",
        statements
    );
    assert!(
        !statements.iter().any(|s| s.contains("hunter2")),
        "the password is not: {:?}",
        statements
    );

    // --- PostgreSQL's own spelling of the reset -----------------------------
    run(&pool, "SELECT pg_stat_statements_reset()")
        .await
        .expect("the function the extension offers works too");
    assert!(
        rows(&pool, "SELECT query FROM pg_stat_statements")
            .await
            .iter()
            .all(|r| !r.get::<String, _>("query").contains("logtest")),
        "the aggregate was emptied"
    );
}

