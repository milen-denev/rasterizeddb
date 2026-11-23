//! What the statement log says about a statement `statement_timeout` killed.
//!
//! Two features meet here and neither owns the result. The timeout stops a scan
//! part-way; the statement log records what the statement did. The combination
//! has one failure mode worth pinning: if a cancelled scan came back as an
//! *empty success*, the log would faithfully record `rows 0, scanned 10M,
//! status ok` — which reads as a query that legitimately matched nothing, and is
//! indistinguishable from one. The timeout must fail the statement, and the log
//! must record it as a failure.
//!
//! The other half is what makes the row useful: `rows_scanned` counts pointers
//! the scan actually dispatched, so a timed-out statement reports how much of
//! the table it had read when it was cut off. That is the number an operator
//! wants after a timeout, and it is only there because the counter is not
//! rolled back when the statement fails.

use rasterizeddb_compat::TestServer;
use sqlx::AssertSqlSafe;
use sqlx::{Executor, Row};

/// Rows to make the scan slow enough that a one-millisecond budget cannot
/// survive it. Comfortably above the point where the timing could go either
/// way — this is a test about *what is recorded*, not about how fast the
/// machine is — while staying cheap enough to load: the scan dispatches
/// `batch_size` (10,000) pointers at a time and the deadline is checked between
/// batches, so this is several checks rather than one.
const ROWS: usize = 60_000;

/// Rows per `INSERT`. The load is the whole cost of this test, so it goes in as
/// few statements as it reasonably can.
const BATCH: usize = 5_000;

#[tokio::test(flavor = "multi_thread")]
async fn a_timed_out_statement_is_recorded_as_one() {
    let server = TestServer::start().await.expect("server should start");
    let pool = server.connect_sqlx().await.expect("should connect");

    // No primary key, deliberately. A unique constraint makes every insert look
    // before it writes, under a write lock, which is most of the cost of
    // loading this table — and nothing here needs the rows to be addressable.
    pool.execute(sqlx::raw_sql(AssertSqlSafe(
        "CREATE TABLE readings (id INT NOT NULL, sensor INT NOT NULL, value INT NOT NULL)",
    )))
    .await
    .expect("create");

    for start in (1..=ROWS).step_by(BATCH) {
        let values: Vec<String> = (start..(start + BATCH).min(ROWS + 1))
            .map(|id| format!("({}, {}, {})", id, id % 97, id))
            .collect();
        pool.execute(sqlx::raw_sql(AssertSqlSafe(format!(
            "INSERT INTO readings (id, sensor, value) VALUES {}",
            values.join(", ")
        ))))
        .await
        .expect("insert");
    }

    // One connection for both statements: the timeout is the session's, so a
    // pooled connection that did not run the SET would not be bound by it.
    let mut conn = pool.acquire().await.expect("acquire");
    conn.execute(sqlx::raw_sql(AssertSqlSafe("SET statement_timeout = '1ms'")))
        .await
        .expect("set");

    // A predicate nothing indexes, over every row: the shape the scan cannot
    // shortcut, and the one a timeout exists to stop.
    let killed = sqlx::query(AssertSqlSafe(
        "SELECT id FROM readings WHERE value > 0 AND sensor > 0",
    ))
    .fetch_all(&mut *conn)
    .await;

    let Err(error) = killed else {
        // The machine outran a one-millisecond budget over 120k rows. Nothing
        // is asserted about the recording in that case, but the test says so
        // rather than passing quietly on a condition it did not test.
        panic!(
            "the scan finished inside 1ms, so the timeout never tripped and this test checked \
             nothing; raise ROWS if this is now normal on this hardware"
        );
    };
    let message = error.to_string();
    assert!(
        message.contains("57014") || message.to_lowercase().contains("cancel")
            || message.to_lowercase().contains("timeout"),
        "a timeout should report itself as one, got: {}",
        message
    );

    conn.execute(sqlx::raw_sql(AssertSqlSafe("SET statement_timeout = 0")))
        .await
        .expect("reset");

    let recorded = sqlx::query(AssertSqlSafe("SHOW QUERIES 20"))
        .fetch_all(&mut *conn)
        .await
        .expect("the log is readable");
    let entry = recorded
        .iter()
        .find(|r| r.get::<String, _>("statement").contains("sensor > 0"))
        .expect("the statement that was killed is in the log");

    // The whole point: a cancelled statement is a failure, not an empty result.
    assert_eq!(
        entry.get::<String, _>("status"),
        "error",
        "a cancelled scan recorded as `ok` is indistinguishable from one that matched nothing"
    );
    assert!(!entry.get::<String, _>("error").is_empty(), "the reason is kept");
    assert_eq!(entry.get::<i64, _>("rows"), 0, "nothing was returned");
    // And how far it got, which is the reason to look at this row at all.
    assert!(
        entry.get::<i64, _>("rows_scanned") > 0,
        "a statement cut off mid-scan should report what it had already read"
    );
}
