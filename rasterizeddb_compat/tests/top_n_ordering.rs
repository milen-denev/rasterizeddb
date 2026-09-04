//! `ORDER BY … LIMIT k` must return the same rows now that each scan worker
//! keeps only its own best `k`.
//!
//! The worker-side trim is a partition, not a sort, and it runs per worker —
//! so the risk it introduces is not "slow" but "wrong rows". Every case here
//! compares against the answer computed from the full ordering.

use sqlx::{AssertSqlSafe, Connection, Executor, Row};
use rasterizeddb_compat::TestServer;

/// Enough rows to span many batches and several workers, and bounded.
const ROWS: i64 = 40_000;

async fn seeded() -> (TestServer, sqlx::PgConnection) {
    let server = TestServer::start().await.expect("start");
    let mut conn = sqlx::PgConnection::connect(&server.url()).await.expect("connect");
    conn.execute(AssertSqlSafe(
        "CREATE TABLE s (id INT PRIMARY KEY, k BIGINT NOT NULL, tie BIGINT NOT NULL, maybe BIGINT)",
    ))
    .await
    .expect("create");

    for chunk in (0..ROWS).collect::<Vec<_>>().chunks(2_000) {
        let values = chunk
            .iter()
            .map(|i| {
                // Scattered so no worker sees a sorted run, plus a heavily
                // tied column: ties are where a partition-based top-N is most
                // likely to disagree with a full sort.
                let k = (i * 7919) % ROWS;
                format!("({i}, {k}, {}, {k})", k % 10)
            })
            .collect::<Vec<_>>()
            .join(",");
        conn.execute(AssertSqlSafe(format!(
            "INSERT INTO s (id, k, tie, maybe) VALUES {values}"
        )))
        .await
        .expect("insert");
    }
    (server, conn)
}

async fn keys(conn: &mut sqlx::PgConnection, sql: &str) -> Vec<i64> {
    sqlx::query(AssertSqlSafe(sql))
        .fetch_all(conn)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .iter()
        .map(|r| r.try_get::<i64, _>(0).expect("key"))
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_limited_ordering_returns_the_same_keys_as_the_full_one() {
    let (_server, mut conn) = seeded().await;

    for (label, order, cap) in [
        ("ascending", "k ASC", 3),
        ("descending", "k DESC", 3),
        ("one row", "k DESC", 1),
        ("larger page", "k ASC", 500),
        ("more than exists", "k ASC", ROWS as usize + 10),
        ("ties then unique", "tie ASC, k ASC", 25),
        ("ties descending", "tie DESC, k DESC", 25),
        ("nullable key", "maybe DESC", 5),
    ] {
        let limited = keys(
            &mut conn,
            &format!("SELECT k FROM s ORDER BY {order} LIMIT {cap}"),
        )
        .await;

        // The same ordering with no cap, cut client-side: the answer the
        // limited form has to agree with.
        let full = keys(&mut conn, &format!("SELECT k FROM s ORDER BY {order}")).await;
        let expected: Vec<i64> = full.into_iter().take(cap).collect();

        assert_eq!(
            limited.len(),
            expected.len(),
            "{label}: wrong row count for LIMIT {cap}"
        );
        assert_eq!(limited, expected, "{label}: LIMIT {cap} returned different keys");
    }
}

/// `OFFSET` pages through the same ordering, so page boundaries must line up.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn paging_with_an_offset_still_walks_the_full_ordering() {
    let (_server, mut conn) = seeded().await;
    let full = keys(&mut conn, "SELECT k FROM s ORDER BY k ASC").await;

    for offset in [0usize, 1, 17, 1_000] {
        let page = keys(
            &mut conn,
            &format!("SELECT k FROM s ORDER BY k ASC LIMIT 10 OFFSET {offset}"),
        )
        .await;
        let expected: Vec<i64> = full.iter().skip(offset).take(10).copied().collect();
        assert_eq!(page, expected, "OFFSET {offset} returned the wrong page");
    }
}

/// Real NULLs, now that the scan sorts nullable columns itself.
///
/// PostgreSQL treats NULL as larger than every value, so it lands last
/// ascending and first descending. Getting this wrong is silent: a NULL stored
/// as a timestamp is the type's zeroes, which byte-sorts as 1970 — a plausible
/// date in the wrong place, not an error.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn nulls_sort_where_postgres_puts_them() {
    let server = TestServer::start().await.expect("start");
    let mut conn = sqlx::PgConnection::connect(&server.url()).await.expect("connect");
    conn.execute(AssertSqlSafe(
        "CREATE TABLE n (id INT PRIMARY KEY, v BIGINT, at TIMESTAMPTZ)",
    ))
    .await
    .expect("create");

    // Interleaved so NULLs are neither first nor last in storage order, and
    // enough rows to cross batch boundaries in more than one worker.
    for chunk in (0..30_000i64).collect::<Vec<_>>().chunks(2_000) {
        let values = chunk
            .iter()
            .map(|i| {
                if i % 3 == 0 {
                    format!("({i}, NULL, NULL)")
                } else {
                    format!("({i}, {}, '2020-01-01 00:00:00+00')", i % 977)
                }
            })
            .collect::<Vec<_>>()
            .join(",");
        conn.execute(AssertSqlSafe(format!(
            "INSERT INTO n (id, v, at) VALUES {values}"
        )))
        .await
        .expect("insert");
    }

    // Ascending: NULLs last, so a small page must contain none of them.
    let head: Vec<Option<i64>> = sqlx::query(AssertSqlSafe("SELECT v FROM n ORDER BY v ASC LIMIT 5"))
        .fetch_all(&mut conn)
        .await
        .expect("asc")
        .iter()
        .map(|r| r.try_get::<Option<i64>, _>(0).expect("v"))
        .collect();
    assert!(
        head.iter().all(|v| v.is_some()),
        "ascending must put NULLs last, got {head:?}"
    );
    assert_eq!(head.first().copied().flatten(), Some(0), "smallest value first");

    // Descending: NULLs first.
    let head: Vec<Option<i64>> =
        sqlx::query(AssertSqlSafe("SELECT v FROM n ORDER BY v DESC LIMIT 5"))
            .fetch_all(&mut conn)
            .await
            .expect("desc")
            .iter()
            .map(|r| r.try_get::<Option<i64>, _>(0).expect("v"))
            .collect();
    assert!(
        head.iter().all(|v| v.is_none()),
        "descending must put NULLs first, got {head:?}"
    );

    // A NULL timestamp must not sort as 1970 — the failure this guards.
    let head: Vec<Option<chrono::DateTime<chrono::Utc>>> =
        sqlx::query(AssertSqlSafe("SELECT at FROM n ORDER BY at ASC LIMIT 5"))
            .fetch_all(&mut conn)
            .await
            .expect("ts asc")
            .iter()
            .map(|r| r.try_get::<Option<chrono::DateTime<chrono::Utc>>, _>(0).expect("at"))
            .collect();
    assert!(
        head.iter().all(|v| v.is_some()),
        "a NULL timestamp must sort last ascending, not as the epoch: {head:?}"
    );

    // And the whole ordering still agrees with the uncapped one.
    let full: Vec<Option<i64>> = sqlx::query(AssertSqlSafe("SELECT v FROM n ORDER BY v ASC"))
        .fetch_all(&mut conn)
        .await
        .expect("full")
        .iter()
        .map(|r| r.try_get::<Option<i64>, _>(0).expect("v"))
        .collect();
    let limited: Vec<Option<i64>> =
        sqlx::query(AssertSqlSafe("SELECT v FROM n ORDER BY v ASC LIMIT 100"))
            .fetch_all(&mut conn)
            .await
            .expect("limited")
            .iter()
            .map(|r| r.try_get::<Option<i64>, _>(0).expect("v"))
            .collect();
    assert_eq!(limited, full.into_iter().take(100).collect::<Vec<_>>());
}

/// The columns that are *not* the sort key must belong to the row that won.
///
/// The scan no longer fetches the payload while it sorts: with a top-N in
/// force it reads only the sort key, keeps the pointer, and reads the wide
/// columns afterwards for the rows that survived. That is worth ~5x on a wide
/// table, and it introduces a failure this file could not previously see —
/// every test above selects the sort key alone, so a payload stitched back to
/// the *wrong* pointer would return perfectly correct keys and quietly wrong
/// rows.
///
/// The fixture makes each row self-describing: `k` is a bijection of `id`,
/// `tie` is `k % 10`, and `maybe` mirrors `k`. So a mismatch is detectable
/// from the row alone, without trusting the ordering to be right.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_payload_belongs_to_the_row_that_won() {
    let (_server, mut conn) = seeded().await;

    // A nullable key that is genuinely NULL for some rows: the narrow fetch
    // has to carry the `__null_x` companion, and a key read without it sorts
    // by stored bytes instead.
    conn.execute(AssertSqlSafe(
        "INSERT INTO s (id, k, tie, maybe) VALUES \
         (900001, 900001, 3, NULL), (900002, 900002, 4, NULL)",
    ))
    .await
    .expect("null rows");

    for (label, order, cap) in [
        ("ascending", "k ASC", 3),
        ("descending", "k DESC", 3),
        ("one row", "k DESC", 1),
        ("larger page", "k ASC", 500),
        ("ties then unique", "tie ASC, k ASC", 25),
        ("ties descending", "tie DESC, k DESC", 25),
        // `id` breaks every tie, so the ordering is total. Without it the two
        // NULL rows below are interchangeable, and the capped path (which
        // partitions) may legitimately return them in the other order from the
        // uncapped path (which sorts) — a tie, not a defect.
        ("nullable key", "maybe DESC, id ASC", 5),
        ("nullable ascending", "maybe ASC, id ASC", 5),
    ] {
        let sql = format!("SELECT id, k, tie, maybe FROM s ORDER BY {order} LIMIT {cap}");
        let rows = sqlx::query(AssertSqlSafe(sql.clone()))
            .fetch_all(&mut conn)
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));

        assert_eq!(rows.len(), cap.min(ROWS as usize + 2), "{label}: wrong row count");

        // The decisive check. Internal consistency alone cannot catch a
        // mispairing: a payload read through the wrong pointer returns a
        // genuine row, which is self-consistent and simply the wrong one. Only
        // comparing against the same ordering uncapped shows that.
        let full_sql = format!("SELECT id, k, tie, maybe FROM s ORDER BY {order}");
        let expected: Vec<(i64, i64, i64, Option<i64>)> =
            sqlx::query(AssertSqlSafe(full_sql.clone()))
                .fetch_all(&mut conn)
                .await
                .unwrap_or_else(|e| panic!("{full_sql}: {e}"))
                .iter()
                .take(cap)
                .map(|r| {
                    (
                        r.try_get::<i32, _>(0).expect("id") as i64,
                        r.try_get::<i64, _>(1).expect("k"),
                        r.try_get::<i64, _>(2).expect("tie"),
                        r.try_get::<Option<i64>, _>(3).expect("maybe"),
                    )
                })
                .collect();
        let got: Vec<(i64, i64, i64, Option<i64>)> = rows
            .iter()
            .map(|r| {
                (
                    r.try_get::<i32, _>(0).expect("id") as i64,
                    r.try_get::<i64, _>(1).expect("k"),
                    r.try_get::<i64, _>(2).expect("tie"),
                    r.try_get::<Option<i64>, _>(3).expect("maybe"),
                )
            })
            .collect();
        assert_eq!(got, expected, "{label}: LIMIT {cap} returned different rows");

        for row in &rows {
            let id = row.try_get::<i32, _>(0).expect("id") as i64;
            let k = row.try_get::<i64, _>(1).expect("k");
            let tie = row.try_get::<i64, _>(2).expect("tie");
            let maybe = row.try_get::<Option<i64>, _>(3).expect("maybe");

            // The two rows added above are outside the generated pattern.
            if id >= 900_001 {
                assert_eq!(k, id, "{label}: added row {id} came back with k={k}");
                assert_eq!(maybe, None, "{label}: added row {id} should have a NULL key");
                continue;
            }

            assert_eq!(
                k,
                (id * 7919) % ROWS,
                "{label}: row {id} carries another row's k ({k}) — \
                 the payload was stitched to the wrong pointer"
            );
            assert_eq!(tie, k % 10, "{label}: row {id} carries another row's tie ({tie})");
            assert_eq!(
                maybe,
                Some(k),
                "{label}: row {id} carries another row's nullable column ({maybe:?})"
            );
        }
    }
}

/// The same, for the filtered path — a `WHERE` reaches different fetch code.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_filtered_top_n_also_keeps_its_payload() {
    let (_server, mut conn) = seeded().await;

    for (label, predicate, order, cap) in [
        ("selective", "k < 500", "k ASC", 10),
        ("most rows match", "k >= 0", "k DESC", 10),
        ("on a non-key column", "tie = 3", "k ASC", 10),
    ] {
        let sql = format!(
            "SELECT id, k, tie FROM s WHERE {predicate} ORDER BY {order} LIMIT {cap}"
        );
        let rows = sqlx::query(AssertSqlSafe(sql.clone()))
            .fetch_all(&mut conn)
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
        assert!(!rows.is_empty(), "{label}: returned nothing");

        // Against the same predicate and ordering uncapped. Internal
        // consistency alone cannot see a mispairing — a payload read through
        // the wrong pointer is still a real row.
        let full_sql =
            format!("SELECT id, k, tie FROM s WHERE {predicate} ORDER BY {order}");
        let expected: Vec<(i64, i64, i64)> = sqlx::query(AssertSqlSafe(full_sql.clone()))
            .fetch_all(&mut conn)
            .await
            .unwrap_or_else(|e| panic!("{full_sql}: {e}"))
            .iter()
            .take(cap)
            .map(|r| {
                (
                    r.try_get::<i32, _>(0).expect("id") as i64,
                    r.try_get::<i64, _>(1).expect("k"),
                    r.try_get::<i64, _>(2).expect("tie"),
                )
            })
            .collect();
        let got: Vec<(i64, i64, i64)> = rows
            .iter()
            .map(|r| {
                (
                    r.try_get::<i32, _>(0).expect("id") as i64,
                    r.try_get::<i64, _>(1).expect("k"),
                    r.try_get::<i64, _>(2).expect("tie"),
                )
            })
            .collect();
        assert_eq!(got, expected, "{label}: LIMIT {cap} returned different rows");

        for row in &rows {
            let id = row.try_get::<i32, _>(0).expect("id") as i64;
            let k = row.try_get::<i64, _>(1).expect("k");
            let tie = row.try_get::<i64, _>(2).expect("tie");
            assert_eq!(
                k,
                (id * 7919) % ROWS,
                "{label}: row {id} carries another row's k ({k})"
            );
            assert_eq!(tie, k % 10, "{label}: row {id} carries another row's tie ({tie})");
        }
    }
}
