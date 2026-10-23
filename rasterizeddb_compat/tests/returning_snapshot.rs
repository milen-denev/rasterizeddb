//! `DELETE … RETURNING` and `UPDATE … RETURNING` report exactly the rows the
//! statement changed, even while other transactions commit around it.
//!
//! RETURNING used to be read before the write, under a snapshot of its own.
//! A row inserted (or deleted) by a transaction committing between that read
//! and the write's own snapshot was then deleted but not reported, or
//! reported but not deleted. The client was told about rows the statement
//! never touched, and nothing errored.
//!
//! The check is an accounting identity rather than a timing: while one task
//! keeps inserting rows that match, another keeps deleting them with
//! `RETURNING`. Every inserted id must come back from exactly one DELETE, or
//! still be in the table at the end.

use rasterizeddb_compat::TestServer;
use sqlx::{Executor, Row};
use std::collections::BTreeSet;
use std::time::{Duration, Instant};

#[tokio::test(flavor = "multi_thread")]
async fn delete_returning_reports_exactly_the_rows_it_deleted() {
    let server = TestServer::start().await.expect("server");
    let pool = server.connect_sqlx().await.expect("connect");
    pool.execute(sqlx::raw_sql("CREATE TABLE q (id BIGINT NOT NULL, state TEXT NOT NULL)"))
        .await
        .expect("create");

    const INSERTS: i64 = 1500;
    let inserter = {
        let pool = pool.clone();
        tokio::spawn(async move {
            for id in 0..INSERTS {
                sqlx::query("INSERT INTO q (id, state) VALUES ($1, 'ready')")
                    .bind(id)
                    .execute(&pool)
                    .await
                    .expect("insert");
            }
        })
    };

    let mut returned: Vec<i64> = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let rows = sqlx::query("DELETE FROM q WHERE state = 'ready' RETURNING id")
            .fetch_all(&pool)
            .await
            .expect("delete");
        returned.extend(rows.iter().map(|r| r.get::<i64, _>(0)));
        if inserter.is_finished() {
            break;
        }
        assert!(Instant::now() < deadline, "the inserter did not finish in time");
    }
    inserter.await.expect("inserter");

    let remaining: BTreeSet<i64> = sqlx::query("SELECT id FROM q")
        .fetch_all(&pool)
        .await
        .expect("remaining")
        .iter()
        .map(|r| r.get::<i64, _>(0))
        .collect();
    let returned_set: BTreeSet<i64> = returned.iter().copied().collect();
    assert_eq!(returned.len(), returned_set.len(), "a row was reported as deleted twice");
    let both: Vec<i64> = returned_set.intersection(&remaining).copied().collect();
    assert!(both.is_empty(), "reported as deleted but still in the table: {both:?}");
    let missing: Vec<i64> = (0..INSERTS)
        .filter(|id| !returned_set.contains(id) && !remaining.contains(id))
        .collect();
    assert!(missing.is_empty(), "deleted without being reported: {} rows, e.g. {:?}", missing.len(), &missing[..missing.len().min(10)]);
}

#[tokio::test(flavor = "multi_thread")]
async fn update_returning_reports_exactly_the_rows_it_updated() {
    let server = TestServer::start().await.expect("server");
    let pool = server.connect_sqlx().await.expect("connect");
    pool.execute(sqlx::raw_sql("CREATE TABLE j (id BIGINT NOT NULL, state TEXT NOT NULL, n INT NOT NULL)"))
        .await
        .expect("create");

    const INSERTS: i64 = 1500;
    let inserter = {
        let pool = pool.clone();
        tokio::spawn(async move {
            for id in 0..INSERTS {
                sqlx::query("INSERT INTO j (id, state, n) VALUES ($1, 'ready', 1)")
                    .bind(id)
                    .execute(&pool)
                    .await
                    .expect("insert");
            }
        })
    };

    // Each claim moves a row out of 'ready', so a row is claimed at most once;
    // RETURNING reports the values after the change.
    let mut claimed: Vec<(i64, String, i32)> = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let rows = sqlx::query("UPDATE j SET state = 'taken', n = n + 1 WHERE state = 'ready' RETURNING id, state, n")
            .fetch_all(&pool)
            .await
            .expect("update");
        claimed.extend(rows.iter().map(|r| (r.get::<i64, _>(0), r.get::<String, _>(1), r.get::<i32, _>(2))));
        if inserter.is_finished() {
            break;
        }
        assert!(Instant::now() < deadline, "the inserter did not finish in time");
    }
    inserter.await.expect("inserter");
    // Claim whatever the last round left.
    let rows = sqlx::query("UPDATE j SET state = 'taken', n = n + 1 WHERE state = 'ready' RETURNING id, state, n")
        .fetch_all(&pool)
        .await
        .expect("final update");
    claimed.extend(rows.iter().map(|r| (r.get::<i64, _>(0), r.get::<String, _>(1), r.get::<i32, _>(2))));

    for (id, state, n) in &claimed {
        assert_eq!((state.as_str(), *n), ("taken", 2), "row {id} reported with values it was not given");
    }
    let ids: BTreeSet<i64> = claimed.iter().map(|c| c.0).collect();
    assert_eq!(ids.len(), claimed.len(), "a row was reported as updated twice");
    let taken: BTreeSet<i64> = sqlx::query("SELECT id FROM j WHERE state = 'taken'")
        .fetch_all(&pool)
        .await
        .expect("taken")
        .iter()
        .map(|r| r.get::<i64, _>(0))
        .collect();
    assert_eq!(taken, ids, "the rows reported as updated are not the rows the table shows updated");
    assert_eq!(taken.len() as i64, INSERTS, "every row was claimed");
}
