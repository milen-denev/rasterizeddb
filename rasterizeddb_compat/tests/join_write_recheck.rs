//! `DELETE … USING` and `UPDATE … FROM` recheck their join condition against
//! each target row as it is when written, and count only what they changed.
//!
//! Both run the join first and then write each target row back by its key.
//! The write used to name the key alone. A row another transaction changed
//! after the join (moved out of the condition) was still written, and the
//! statement counted one per key even when the row was already gone.
//! PostgreSQL's READ COMMITTED rechecks the condition on the row's newest
//! version with the joined source row, and skips it if it no longer holds.
//!
//! Both checks are invariants a concurrent run cannot fake: a row a DELETE
//! reports must still have matched when deleted, and the counts reported by
//! racing deletes add up to exactly the rows there were.

use rasterizeddb_compat::TestServer;
use sqlx::{Executor, Row};
use std::time::{Duration, Instant};

async fn setup(pool: &sqlx::PgPool, rows: i64) {
    pool.execute(sqlx::raw_sql(
        "CREATE TABLE users (id BIGINT PRIMARY KEY, banned BOOLEAN NOT NULL);
         INSERT INTO users VALUES (1, true), (2, false);
         CREATE TABLE t (id BIGINT PRIMARY KEY, uid BIGINT NOT NULL);",
    ))
    .await
    .expect("schema");
    for id in 0..rows {
        sqlx::query("INSERT INTO t (id, uid) VALUES ($1, 1)")
            .bind(id)
            .execute(pool)
            .await
            .expect("seed");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn delete_using_never_deletes_a_row_moved_out_of_the_join() {
    let server = TestServer::start().await.expect("server");
    let pool = server.connect_sqlx().await.expect("connect");
    const ROWS: i64 = 600;
    setup(&pool, ROWS).await;

    // Moves rows from the banned user to the other one, one commit each.
    let mover = {
        let pool = pool.clone();
        tokio::spawn(async move {
            for id in (0..ROWS).rev() {
                sqlx::query("UPDATE t SET uid = 2 WHERE id = $1")
                    .bind(id)
                    .execute(&pool)
                    .await
                    .expect("move");
            }
        })
    };

    let mut deleted: Vec<(i64, i64)> = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(90);
    while !mover.is_finished() {
        let rows = sqlx::query("DELETE FROM t USING users u WHERE t.uid = u.id AND u.banned RETURNING t.id, t.uid")
            .fetch_all(&pool)
            .await
            .expect("delete using");
        deleted.extend(rows.iter().map(|r| (r.get::<i64, _>(0), r.get::<i64, _>(1))));
        assert!(Instant::now() < deadline, "the mover did not finish in time");
    }
    mover.await.expect("mover");

    let moved_then_deleted: Vec<&(i64, i64)> = deleted.iter().filter(|(_, uid)| *uid != 1).collect();
    assert!(
        moved_then_deleted.is_empty(),
        "deleted rows that no longer joined a banned user: {} of {}, e.g. {:?}",
        moved_then_deleted.len(),
        deleted.len(),
        &moved_then_deleted[..moved_then_deleted.len().min(5)]
    );
    // Whatever was not deleted was moved, and survives.
    let left: i64 = sqlx::query("SELECT COUNT(*) FROM t").fetch_one(&pool).await.expect("count").get(0);
    assert_eq!(left + deleted.len() as i64, ROWS, "rows were lost or reported twice");
}

#[tokio::test(flavor = "multi_thread")]
async fn racing_deletes_report_exactly_the_rows_there_were() {
    let server = TestServer::start().await.expect("server");
    let pool = server.connect_sqlx().await.expect("connect");
    const ROWS: i64 = 600;
    setup(&pool, ROWS).await;

    // A plain delete by key, racing the join's own deletes over the same rows.
    let plain = {
        let pool = pool.clone();
        tokio::spawn(async move {
            let mut count = 0u64;
            for id in (0..ROWS).rev() {
                count += sqlx::query("DELETE FROM t WHERE id = $1")
                    .bind(id)
                    .execute(&pool)
                    .await
                    .expect("plain delete")
                    .rows_affected();
            }
            count
        })
    };

    let mut joined = 0u64;
    let deadline = Instant::now() + Duration::from_secs(90);
    while !plain.is_finished() {
        joined += sqlx::query("DELETE FROM t USING users u WHERE t.uid = u.id AND u.banned")
            .execute(&pool)
            .await
            .expect("delete using")
            .rows_affected();
        assert!(Instant::now() < deadline, "the plain deleter did not finish in time");
    }
    let plain = plain.await.expect("plain");
    let left: i64 = sqlx::query("SELECT COUNT(*) FROM t").fetch_one(&pool).await.expect("count").get(0);
    assert_eq!(left, 0, "every row was deleted by one side or the other");
    assert_eq!(
        plain + joined,
        ROWS as u64,
        "deletes reported {} (plain {} + joined {}) for {} rows",
        plain + joined,
        plain,
        joined,
        ROWS
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn update_from_never_updates_a_row_moved_out_of_the_join() {
    let server = TestServer::start().await.expect("server");
    let pool = server.connect_sqlx().await.expect("connect");
    const ROWS: i64 = 600;
    setup(&pool, ROWS).await;
    pool.execute(sqlx::raw_sql("ALTER TABLE t ADD COLUMN flagged BOOLEAN NOT NULL DEFAULT false"))
        .await
        .expect("add column");

    let mover = {
        let pool = pool.clone();
        tokio::spawn(async move {
            for id in (0..ROWS).rev() {
                sqlx::query("UPDATE t SET uid = 2 WHERE id = $1")
                    .bind(id)
                    .execute(&pool)
                    .await
                    .expect("move");
            }
        })
    };

    let mut flagged: Vec<(i64, i64)> = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(90);
    while !mover.is_finished() {
        let rows = sqlx::query(
            "UPDATE t SET flagged = true FROM users u WHERE t.uid = u.id AND u.banned AND NOT t.flagged RETURNING t.id, t.uid",
        )
        .fetch_all(&pool)
        .await
        .expect("update from");
        flagged.extend(rows.iter().map(|r| (r.get::<i64, _>(0), r.get::<i64, _>(1))));
        assert!(Instant::now() < deadline, "the mover did not finish in time");
    }
    mover.await.expect("mover");

    let wrong: Vec<&(i64, i64)> = flagged.iter().filter(|(_, uid)| *uid != 1).collect();
    assert!(
        wrong.is_empty(),
        "flagged rows that no longer joined a banned user: {} of {}, e.g. {:?}",
        wrong.len(),
        flagged.len(),
        &wrong[..wrong.len().min(5)]
    );
}
