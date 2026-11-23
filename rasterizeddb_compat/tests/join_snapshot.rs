//! A join reads every relation as of one moment, the statement's snapshot.
//!
//! Each relation used to take a snapshot of its own when its scan began. A
//! transaction committing between two scans then showed in one relation and
//! not the other: a transfer from one table to another, joined back, came out
//! as half a transfer, and nothing errored.
//!
//! The check is an invariant: a transfer moves units between `a` and `b` in
//! one transaction, so every joined row must show the same total. `a` carries
//! filler rows so its scan is long enough for commits to land during it.

use rasterizeddb_compat::TestServer;
use sqlx::{AssertSqlSafe, Executor, Row};
use std::time::{Duration, Instant};

#[tokio::test(flavor = "multi_thread")]
async fn a_join_never_shows_half_a_transaction() {
    let server = TestServer::start().await.expect("server");
    let pool = server.connect_sqlx().await.expect("connect");
    pool.execute(sqlx::raw_sql(
        "CREATE TABLE a (id BIGINT PRIMARY KEY, bal BIGINT NOT NULL);
         CREATE TABLE b (id BIGINT PRIMARY KEY, bal BIGINT NOT NULL);
         INSERT INTO a VALUES (1, 1000);
         INSERT INTO b VALUES (1, 0);",
    ))
    .await
    .expect("schema");
    for batch in 0..40i64 {
        let values: Vec<String> = (0..500i64).map(|i| format!("({}, 0)", 1000 + batch * 500 + i)).collect();
        pool.execute(AssertSqlSafe(format!("INSERT INTO a VALUES {}", values.join(", "))))
            .await
            .expect("filler");
    }

    const TRANSFERS: i64 = 300;
    let mover = {
        let pool = pool.clone();
        tokio::spawn(async move {
            for _ in 0..TRANSFERS {
                let mut tx = pool.begin().await.expect("begin");
                sqlx::query("UPDATE a SET bal = bal - 1 WHERE id = 1").execute(&mut *tx).await.expect("debit");
                sqlx::query("UPDATE b SET bal = bal + 1 WHERE id = 1").execute(&mut *tx).await.expect("credit");
                tx.commit().await.expect("commit");
            }
        })
    };

    let mut torn: Vec<(i64, i64)> = Vec::new();
    let mut reads = 0u64;
    let deadline = Instant::now() + Duration::from_secs(120);
    while !mover.is_finished() {
        let rows = sqlx::query("SELECT a.bal, b.bal FROM a JOIN b ON a.id = b.id")
            .fetch_all(&pool)
            .await
            .expect("join");
        assert_eq!(rows.len(), 1, "the join matches exactly one row");
        let (x, y) = (rows[0].get::<i64, _>(0), rows[0].get::<i64, _>(1));
        if x + y != 1000 {
            torn.push((x, y));
        }
        reads += 1;
        assert!(Instant::now() < deadline, "the mover did not finish in time");
    }
    mover.await.expect("mover");

    assert!(
        torn.is_empty(),
        "{} of {} joins saw half a transfer, e.g. {:?}",
        torn.len(),
        reads,
        &torn[..torn.len().min(5)]
    );
    let row = sqlx::query("SELECT a.bal, b.bal FROM a JOIN b ON a.id = b.id").fetch_one(&pool).await.expect("final");
    assert_eq!((row.get::<i64, _>(0), row.get::<i64, _>(1)), (1000 - TRANSFERS, TRANSFERS));
}
