//! Row-addressing statements on tables whose only unique keys span several
//! columns (`docs/partitioning.md` §1b, phase 3 P3): every partitioned table,
//! and a plain table with a composite `PRIMARY KEY`.
//!
//! `FOR UPDATE`/`SHARE`, `SKIP LOCKED` and `NOWAIT` lock a row by the framed
//! tuple of its identifying key, under the table's logical name, so a lock
//! taken through any partition is the one a writer through the logical name
//! waits for. `UPDATE … FROM` and `DELETE … USING` join, then write each
//! target row back by that key, routed to its partition, under the join's one
//! snapshot.
//!
//! Every test runs on a plain table and a partitioned one with the same
//! columns, over pgwire, bounded, and reads every row back after writing:
//! the failure these guard against reports success and stores the wrong
//! bytes.

use rasterizeddb_compat::TestServer;
use sqlx::{AssertSqlSafe, Connection, Executor, PgConnection, Row};
use std::collections::{BTreeMap, BTreeSet};
use std::time::{Duration, Instant};

/// A file-backed server's root, removed on drop (after the server, which is
/// declared after it). Partitioned tables need a file-backed database.
struct Root(String);

impl Drop for Root {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn root(tag: &str) -> Root {
    let r = std::env::temp_dir().join(format!("composite_row_locks_{}_{tag}", std::process::id()));
    let _ = std::fs::remove_dir_all(&r);
    std::fs::create_dir_all(&r).expect("root");
    Root(r.to_string_lossy().into_owned())
}

/// The two table kinds every test runs on: the same columns and key, one of
/// them partitioned by its leading key column.
const KINDS: [(&str, &str); 2] = [("plain", ""), ("part", " PARTITION BY HASH (tenant_id) PARTITIONS 8")];

const TENANTS: i64 = 5;
const IDS: i64 = 40;

async fn connect(server: &TestServer) -> PgConnection {
    PgConnection::connect(&server.url()).await.expect("connect")
}

async fn exec(conn: &mut PgConnection, sql: &str) -> Result<u64, sqlx::Error> {
    conn.execute(AssertSqlSafe(sql.to_string())).await.map(|r| r.rows_affected())
}

fn code(e: &sqlx::Error) -> Option<String> {
    e.as_database_error().and_then(|d| d.code()).map(|c| c.to_string())
}

async fn keys(conn: &mut PgConnection, sql: &str) -> Result<Vec<(i64, i64)>, sqlx::Error> {
    let rows = sqlx::query(AssertSqlSafe(sql.to_string())).fetch_all(&mut *conn).await?;
    Ok(rows.iter().map(|r| (r.get::<i64, _>(0), r.get::<i64, _>(1))).collect())
}

/// A queue `q_<kind>`: `TENANTS` × `IDS` jobs, all `'new'`.
async fn queue(conn: &mut PgConnection, kind: &str, partitioning: &str) -> String {
    let t = format!("q_{kind}");
    exec(
        conn,
        &format!(
            "CREATE TABLE {t} (tenant_id BIGINT NOT NULL, id BIGINT NOT NULL, state TEXT NOT NULL, \
             claimed_by BIGINT, PRIMARY KEY (tenant_id, id)){partitioning}"
        ),
    )
    .await
    .unwrap_or_else(|e| panic!("create {t}: {e}"));
    for tenant in 1..=TENANTS {
        let values: Vec<String> = (1..=IDS).map(|id| format!("({tenant}, {id}, 'new')")).collect();
        exec(conn, &format!("INSERT INTO {t} (tenant_id, id, state) VALUES {}", values.join(", ")))
            .await
            .unwrap_or_else(|e| panic!("seed {t}: {e}"));
    }
    t
}

async fn states(conn: &mut PgConnection, t: &str) -> BTreeMap<(i64, i64), (String, Option<i64>)> {
    sqlx::query(AssertSqlSafe(format!("SELECT tenant_id, id, state, claimed_by FROM {t}")))
        .fetch_all(&mut *conn)
        .await
        .expect("read back")
        .iter()
        .map(|r| ((r.get::<i64, _>(0), r.get::<i64, _>(1)), (r.get::<String, _>(2), r.get::<Option<i64>, _>(3))))
        .collect()
}

/// A held composite key is held whole and only whole: `NOWAIT` on it is
/// 55P03 (routed, fanned out, and `FOR SHARE`), a key sharing its first
/// column is free, and `SKIP LOCKED` leaves out exactly the held row.
#[tokio::test(flavor = "multi_thread")]
async fn nowait_and_skip_locked_see_the_whole_composite_key() {
    let dir = root("nowait");
    let server = TestServer::start_file_backed(&dir.0).await.expect("server");
    let mut setup = connect(&server).await;
    for (kind, partitioning) in KINDS {
        let t = queue(&mut setup, kind, partitioning).await;
        let before = states(&mut setup, &t).await;

        let mut a = connect(&server).await;
        exec(&mut a, "BEGIN").await.unwrap();
        let held = keys(&mut a, &format!("SELECT tenant_id, id FROM {t} WHERE tenant_id = 1 AND id = 1 FOR UPDATE"))
            .await
            .unwrap_or_else(|e| panic!("{t}: lock: {e}"));
        assert_eq!(held, vec![(1, 1)], "{t}");

        let mut b = connect(&server).await;
        for (what, sql) in [
            ("routed", format!("SELECT tenant_id, id FROM {t} WHERE tenant_id = 1 AND id = 1 FOR UPDATE NOWAIT")),
            ("fanned out", format!("SELECT tenant_id, id FROM {t} WHERE id = 1 FOR UPDATE NOWAIT")),
            ("for share", format!("SELECT tenant_id, id FROM {t} WHERE tenant_id = 1 AND id = 1 FOR SHARE NOWAIT")),
        ] {
            exec(&mut b, "BEGIN").await.unwrap();
            let e = keys(&mut b, &sql).await.expect_err(&format!("{t} {what}: NOWAIT on a held row succeeded"));
            assert_eq!(code(&e).as_deref(), Some("55P03"), "{t} {what}: {e}");
            exec(&mut b, "ROLLBACK").await.unwrap();
        }

        // Keys sharing a column with the held one are other rows.
        for (tenant, id) in [(1, 2), (2, 1)] {
            exec(&mut b, "BEGIN").await.unwrap();
            let got = keys(
                &mut b,
                &format!("SELECT tenant_id, id FROM {t} WHERE tenant_id = {tenant} AND id = {id} FOR UPDATE NOWAIT"),
            )
            .await
            .unwrap_or_else(|e| panic!("{t} ({tenant}, {id}): another row's lock was refused: {e}"));
            assert_eq!(got, vec![(tenant, id)], "{t}");
            exec(&mut b, "ROLLBACK").await.unwrap();
        }

        exec(&mut b, "BEGIN").await.unwrap();
        let mut got = keys(&mut b, &format!("SELECT tenant_id, id FROM {t} WHERE tenant_id = 1 FOR UPDATE SKIP LOCKED"))
            .await
            .unwrap_or_else(|e| panic!("{t}: skip locked: {e}"));
        got.sort();
        let want: Vec<(i64, i64)> = (2..=IDS).map(|id| (1, id)).collect();
        assert_eq!(got, want, "{t}: SKIP LOCKED must leave out exactly the held row");
        exec(&mut b, "ROLLBACK").await.unwrap();

        exec(&mut a, "COMMIT").await.unwrap();
        let got = keys(&mut b, &format!("SELECT tenant_id, id FROM {t} WHERE tenant_id = 1 AND id = 1 FOR UPDATE NOWAIT"))
            .await
            .unwrap_or_else(|e| panic!("{t}: the lock outlived its transaction: {e}"));
        assert_eq!(got, vec![(1, 1)], "{t}");
        assert_eq!(states(&mut setup, &t).await, before, "{t}: a locking read changed a row");
    }
}

/// A `FOR UPDATE` holder refuses an `UPDATE` of the same row through another
/// session, routed or fanned out, at once and until it commits (a writer
/// does not wait: it could not re-evaluate what it writes after the wait); a
/// row sharing the held key's first column is written at once.
#[tokio::test(flavor = "multi_thread")]
async fn a_held_row_refuses_a_writer_until_commit() {
    let dir = root("refuses");
    let server = TestServer::start_file_backed(&dir.0).await.expect("server");
    let mut setup = connect(&server).await;
    for (kind, partitioning) in KINDS {
        let t = queue(&mut setup, kind, partitioning).await;
        for (what, writer) in [
            ("routed", format!("UPDATE {t} SET state = 'stolen' WHERE tenant_id = 1 AND id = 1 AND state = 'new'")),
            ("fanned out", format!("UPDATE {t} SET state = 'stolen' WHERE id = 1 AND state = 'new'")),
            ("deleted", format!("DELETE FROM {t} WHERE id = 1 AND state = 'new'")),
        ] {
            exec(&mut setup, &format!("UPDATE {t} SET state = 'new' WHERE id = 1")).await.unwrap();
            let before = states(&mut setup, &t).await;
            let mut a = connect(&server).await;
            exec(&mut a, "BEGIN").await.unwrap();
            keys(&mut a, &format!("SELECT tenant_id, id FROM {t} WHERE tenant_id = 1 AND id = 1 FOR UPDATE"))
                .await
                .unwrap_or_else(|e| panic!("{t}: lock: {e}"));

            let mut b = connect(&server).await;
            let started = Instant::now();
            let e = exec(&mut b, &writer).await.expect_err(&format!("{t} {what}: a writer ran past a held row"));
            assert!(e.to_string().contains("is locked by transaction"), "{t} {what}: {e}");
            assert!(started.elapsed() < Duration::from_secs(2), "{t} {what}: the writer waited {:?}", started.elapsed());
            assert_eq!(states(&mut setup, &t).await, before, "{t} {what}: a refused writer wrote");

            // Another row of the same tenant is not held.
            let n = exec(&mut b, &format!("UPDATE {t} SET claimed_by = 7 WHERE tenant_id = 1 AND id = 2"))
                .await
                .unwrap_or_else(|e| panic!("{t} {what}: a row sharing the held key's first column: {e}"));
            assert_eq!(n, 1, "{t} {what}");

            exec(&mut a, &format!("UPDATE {t} SET state = 'claimed' WHERE tenant_id = 1 AND id = 1"))
                .await
                .unwrap_or_else(|e| panic!("{t}: the holder's own write: {e}"));
            exec(&mut a, "COMMIT").await.unwrap();
            // Released: the writer runs, and finds (1, 1) claimed.
            let n = exec(&mut b, &writer).await.unwrap_or_else(|e| panic!("{t} {what}: after COMMIT: {e}"));
            let after = states(&mut setup, &t).await;
            assert_eq!(after.get(&(1, 1)).map(|s| s.0.as_str()), Some("claimed"), "{t} {what}");
            let others: u64 = if what == "routed" { 0 } else { (TENANTS - 1) as u64 };
            assert_eq!(n, others, "{t} {what}: the writer's count");
            assert_eq!(after[&(1, 2)].1, Some(7), "{t} {what}");
            if what == "deleted" {
                assert_eq!(after.len() as i64, TENANTS * IDS - (TENANTS - 1), "{t}");
                for tenant in 2..=TENANTS {
                    exec(&mut setup, &format!("INSERT INTO {t} (tenant_id, id, state) VALUES ({tenant}, 1, 'new')"))
                        .await
                        .unwrap();
                }
            }
        }
    }
}

/// A cycle through a row lock and a table lock, with a locking read (the
/// key in its select list or not) waiting on the row, ends in exactly one
/// 40P01 within a few seconds. A writer never waits on a row lock, so it
/// cannot close such a cycle.
#[tokio::test(flavor = "multi_thread")]
async fn a_cycle_through_a_composite_row_lock_and_a_table_lock_is_one_deadlock() {
    let dir = root("deadlock");
    let server = TestServer::start_file_backed(&dir.0).await.expect("server");
    let mut setup = connect(&server).await;
    exec(&mut setup, "CREATE TABLE other (id INT PRIMARY KEY)").await.expect("other");
    for (kind, partitioning) in KINDS {
        let t = queue(&mut setup, kind, partitioning).await;
        for waiter in [
            format!("SELECT tenant_id, id FROM {t} WHERE tenant_id = 3 AND id = 4 FOR UPDATE"),
            format!("SELECT id, claimed_by FROM {t} WHERE id = 4 AND tenant_id = 3 FOR NO KEY UPDATE"),
        ] {
            let mut a = connect(&server).await;
            exec(&mut a, "BEGIN").await.unwrap();
            keys(&mut a, &format!("SELECT tenant_id, id FROM {t} WHERE id = 4 AND tenant_id = 3 FOR UPDATE"))
                .await
                .unwrap_or_else(|e| panic!("{t}: lock: {e}"));
            let mut b = connect(&server).await;
            exec(&mut b, "BEGIN").await.unwrap();
            exec(&mut b, "LOCK TABLE other IN ACCESS EXCLUSIVE MODE").await.expect("lock table");

            let start = Instant::now();
            let hb = tokio::spawn(async move {
                let r = exec(&mut b, &waiter).await;
                let _ = exec(&mut b, "ROLLBACK").await;
                (r, start.elapsed())
            });
            tokio::time::sleep(Duration::from_millis(100)).await;
            let ha = tokio::spawn(async move {
                let r = exec(&mut a, "LOCK TABLE other IN ACCESS EXCLUSIVE MODE").await;
                let _ = exec(&mut a, "ROLLBACK").await;
                (r, start.elapsed())
            });
            let (ra, ta) = tokio::time::timeout(Duration::from_secs(30), ha).await.expect("a hung").expect("join");
            let (rb, tb) = tokio::time::timeout(Duration::from_secs(30), hb).await.expect("b hung").expect("join");
            let failed: Vec<&sqlx::Error> = [&ra, &rb].into_iter().filter_map(|r| r.as_ref().err()).collect();
            assert_eq!(failed.len(), 1, "{t}: exactly one party fails; got {ra:?} / {rb:?}");
            assert_eq!(code(failed[0]).as_deref(), Some("40P01"), "{t}: {}", failed[0]);
            assert!(ta.max(tb) < Duration::from_secs(5), "{t}: the cycle took {:?}", ta.max(tb));
        }
        let after = states(&mut setup, &t).await;
        assert_eq!(after[&(3, 4)], ("new".to_string(), None), "{t}: both blocks rolled back");
    }
}

/// The job-queue pattern: workers claim with `FOR UPDATE SKIP LOCKED LIMIT
/// k` and mark what they claimed by key alone, each in its own block. Every
/// job is claimed exactly once, and what is stored names the worker that
/// claimed it. The mark has no `AND state = 'new'`: only the locking read
/// keeps a worker from a job another has finished, so a lock taken on a row
/// changed since the scan's snapshot must not return the row as scanned.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_skip_locked_job_queue_claims_every_job_exactly_once() {
    const WORKERS: i64 = 6;
    let dir = root("queue");
    let server = TestServer::start_file_backed(&dir.0).await.expect("server");
    let mut setup = connect(&server).await;
    for (kind, partitioning) in KINDS {
        let t = queue(&mut setup, kind, partitioning).await;
        let deadline = Instant::now() + Duration::from_secs(90);
        let mut handles = Vec::new();
        for w in 1..=WORKERS {
            let mut c = connect(&server).await;
            let t = t.clone();
            handles.push(tokio::spawn(async move {
                let mut claimed: Vec<(i64, i64)> = Vec::new();
                let mut empty = 0;
                while empty < 3 {
                    assert!(Instant::now() < deadline, "{t}: worker {w} did not finish in time");
                    exec(&mut c, "BEGIN").await.unwrap();
                    let attempt: Result<Vec<(i64, i64)>, sqlx::Error> = async {
                        let jobs = keys(
                            &mut c,
                            &format!(
                                "SELECT tenant_id, id FROM {t} WHERE state = 'new' LIMIT 4 FOR UPDATE SKIP LOCKED"
                            ),
                        )
                        .await?;
                        let mut mine = Vec::new();
                        for (tenant, id) in jobs {
                            let n = exec(
                                &mut c,
                                &format!(
                                    "UPDATE {t} SET state = 'done', claimed_by = {w} \
                                     WHERE tenant_id = {tenant} AND id = {id}"
                                ),
                            )
                            .await?;
                            if n == 1 {
                                mine.push((tenant, id));
                            }
                        }
                        Ok(mine)
                    }
                    .await;
                    match attempt {
                        Ok(mine) => match exec(&mut c, "COMMIT").await {
                            Ok(_) => {
                                empty = if mine.is_empty() { empty + 1 } else { 0 };
                                claimed.extend(mine);
                            }
                            Err(_) => empty = 0,
                        },
                        Err(_) => {
                            let _ = exec(&mut c, "ROLLBACK").await;
                            empty = 0;
                        }
                    }
                }
                (w, claimed)
            }));
        }
        let mut owner: BTreeMap<(i64, i64), i64> = BTreeMap::new();
        for h in handles {
            let (w, claimed) = h.await.expect("worker");
            for job in claimed {
                if let Some(other) = owner.insert(job, w) {
                    panic!("{t}: job {job:?} claimed by workers {other} and {w}");
                }
            }
        }
        assert_eq!(owner.len() as i64, TENANTS * IDS, "{t}: every job claimed");
        let stored = states(&mut setup, &t).await;
        assert_eq!(stored.len() as i64, TENANTS * IDS, "{t}");
        for (job, (state, by)) in &stored {
            assert_eq!(state, "done", "{t}: job {job:?}");
            assert_eq!(*by, owner.get(job).copied(), "{t}: job {job:?} stored a claimer that did not report it");
        }
    }
}

/// The one-statement claim: a CTE takes `FOR UPDATE SKIP LOCKED` rows and an
/// `UPDATE … FROM` over it marks them, in autocommit, from several workers at
/// once. Every job is returned to exactly one worker, and stored as its.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_cte_claim_returns_every_job_to_exactly_one_worker() {
    const WORKERS: i64 = 5;
    let dir = root("cte");
    let server = TestServer::start_file_backed(&dir.0).await.expect("server");
    let mut setup = connect(&server).await;
    for (kind, partitioning) in KINDS {
        let t = queue(&mut setup, kind, partitioning).await;
        let deadline = Instant::now() + Duration::from_secs(90);
        let mut handles = Vec::new();
        for w in 1..=WORKERS {
            let mut c = connect(&server).await;
            let t = t.clone();
            handles.push(tokio::spawn(async move {
                let claim = format!(
                    "WITH claim AS (SELECT tenant_id, id FROM {t} WHERE state = 'new' LIMIT 3 FOR UPDATE SKIP LOCKED) \
                     UPDATE {t} SET state = 'done', claimed_by = {w} FROM claim \
                     WHERE {t}.tenant_id = claim.tenant_id AND {t}.id = claim.id AND {t}.state = 'new' \
                     RETURNING {t}.tenant_id, {t}.id"
                );
                let (mut claimed, mut empty) = (Vec::new(), 0);
                while empty < 3 {
                    assert!(Instant::now() < deadline, "{t}: worker {w} did not finish in time");
                    match keys(&mut c, &claim).await {
                        Ok(got) => {
                            empty = if got.is_empty() { empty + 1 } else { 0 };
                            claimed.extend(got);
                        }
                        Err(_) => empty = 0,
                    }
                }
                (w, claimed)
            }));
        }
        let mut owner: BTreeMap<(i64, i64), i64> = BTreeMap::new();
        for h in handles {
            let (w, claimed) = h.await.expect("worker");
            for job in claimed {
                if let Some(other) = owner.insert(job, w) {
                    panic!("{t}: job {job:?} returned to workers {other} and {w}");
                }
            }
        }
        assert_eq!(owner.len() as i64, TENANTS * IDS, "{t}: every job claimed");
        for (job, (state, by)) in states(&mut setup, &t).await {
            assert_eq!(state, "done", "{t}: job {job:?}");
            assert_eq!(by, owner.get(&job).copied(), "{t}: job {job:?}");
        }
    }
}

/// `UPDATE … FROM` writes a target row once, whatever number of source rows
/// it joins: its count and `RETURNING` hold each row once, and `n = n + 1`
/// is applied once, on a composite key and on a one-column one.
#[tokio::test(flavor = "multi_thread")]
async fn update_from_applies_once_per_target_row() {
    let dir = root("once");
    let server = TestServer::start_file_backed(&dir.0).await.expect("server");
    let mut c = connect(&server).await;
    exec(&mut c, "CREATE TABLE twice (tenant_id BIGINT NOT NULL, id BIGINT NOT NULL, tag INT NOT NULL)").await.unwrap();
    for tenant in 1..=TENANTS {
        for id in 1..=IDS {
            exec(&mut c, &format!("INSERT INTO twice VALUES ({tenant}, {id}, 1), ({tenant}, {id}, 2)")).await.unwrap();
        }
    }
    let single = ("single", "CREATE TABLE u_single (k BIGINT PRIMARY KEY, tenant_id BIGINT NOT NULL, id BIGINT NOT NULL, n INT NOT NULL)");
    let tables = [
        ("plain", "CREATE TABLE u_plain (tenant_id BIGINT NOT NULL, id BIGINT NOT NULL, n INT NOT NULL, PRIMARY KEY (tenant_id, id))".to_string()),
        ("part", "CREATE TABLE u_part (tenant_id BIGINT NOT NULL, id BIGINT NOT NULL, n INT NOT NULL, PRIMARY KEY (tenant_id, id)) PARTITION BY HASH (tenant_id) PARTITIONS 8".to_string()),
        (single.0, single.1.to_string()),
    ];
    for (kind, create) in tables {
        let t = format!("u_{kind}");
        exec(&mut c, &create).await.unwrap_or_else(|e| panic!("{t}: {e}"));
        for tenant in 1..=TENANTS {
            let values: Vec<String> = (1..=IDS)
                .map(|id| if kind == "single" { format!("({}, {tenant}, {id}, 0)", tenant * 1000 + id) } else { format!("({tenant}, {id}, 0)") })
                .collect();
            let columns = if kind == "single" { "(k, tenant_id, id, n)" } else { "(tenant_id, id, n)" };
            exec(&mut c, &format!("INSERT INTO {t} {columns} VALUES {}", values.join(", "))).await.unwrap();
        }
        let rows = sqlx::query(AssertSqlSafe(format!(
            "UPDATE {t} SET n = {t}.n + 1 FROM twice WHERE {t}.tenant_id = twice.tenant_id AND {t}.id = twice.id \
             AND twice.tenant_id <= 3 RETURNING {t}.tenant_id, {t}.id, {t}.n"
        )))
        .fetch_all(&mut c)
        .await
        .unwrap_or_else(|e| panic!("{t}: {e}"));
        let returned: Vec<(i64, i64, i32)> =
            rows.iter().map(|r| (r.get::<i64, _>(0), r.get::<i64, _>(1), r.get::<i32, _>(2))).collect();
        assert_eq!(returned.len() as i64, 3 * IDS, "{t}: RETURNING held a row more than once: {returned:?}");
        assert!(returned.iter().all(|(_, _, n)| *n == 1), "{t}: {returned:?}");
        let n = exec(
            &mut c,
            &format!("UPDATE {t} SET n = {t}.n + 1 FROM twice WHERE {t}.tenant_id = twice.tenant_id AND {t}.id = twice.id AND twice.tenant_id = 4"),
        )
        .await
        .unwrap_or_else(|e| panic!("{t}: {e}"));
        assert_eq!(n as i64, IDS, "{t}: the count");
        let stored: BTreeMap<(i64, i64), i32> = sqlx::query(AssertSqlSafe(format!("SELECT tenant_id, id, n FROM {t}")))
            .fetch_all(&mut c)
            .await
            .unwrap()
            .iter()
            .map(|r| ((r.get::<i64, _>(0), r.get::<i64, _>(1)), r.get::<i32, _>(2)))
            .collect();
        for ((tenant, id), n) in stored {
            assert_eq!(n, i32::from(tenant <= 4), "{t}: ({tenant}, {id}) applied {n} times");
        }

        // DELETE … USING over the same doubled source deletes and returns each
        // row once.
        let rows = sqlx::query(AssertSqlSafe(format!(
            "DELETE FROM {t} USING twice WHERE {t}.tenant_id = twice.tenant_id AND {t}.id = twice.id \
             AND twice.tenant_id = 2 RETURNING {t}.tenant_id, {t}.id"
        )))
        .fetch_all(&mut c)
        .await
        .unwrap_or_else(|e| panic!("{t}: {e}"));
        assert_eq!(rows.len() as i64, IDS, "{t}: DELETE … USING returned a row more than once");
        let left = keys(&mut c, &format!("SELECT tenant_id, id FROM {t}")).await.unwrap();
        assert_eq!(left.len() as i64, (TENANTS - 1) * IDS, "{t}");
        assert!(left.iter().all(|(tenant, _)| *tenant != 2), "{t}");
    }
}

/// The identifying key is chosen as PostgreSQL would name a row: a `UNIQUE`
/// whose columns are all `NOT NULL` before a one-column `UNIQUE` that may hold
/// NULL, so rows whose nullable column is NULL are still named, locked and
/// written back. A table with only the nullable key refuses its NULL rows one
/// by one rather than naming every NULL row at once.
#[tokio::test(flavor = "multi_thread")]
async fn the_identifying_key_is_never_a_nullable_unique_when_a_whole_key_exists() {
    let dir = root("choice");
    let server = TestServer::start_file_backed(&dir.0).await.expect("server");
    let mut c = connect(&server).await;
    exec(
        &mut c,
        "CREATE TABLE ik (a BIGINT NOT NULL, b BIGINT NOT NULL, u BIGINT UNIQUE, n INT, UNIQUE (a, b))",
    )
    .await
    .unwrap();
    exec(&mut c, "CREATE TABLE ik_src (a BIGINT NOT NULL, b BIGINT NOT NULL, v INT NOT NULL)").await.unwrap();
    for a in 1..=3i64 {
        for b in 1..=4i64 {
            let u = if b % 2 == 0 { "NULL".to_string() } else { (a * 10 + b).to_string() };
            exec(&mut c, &format!("INSERT INTO ik (a, b, u, n) VALUES ({a}, {b}, {u}, 0)")).await.unwrap();
            exec(&mut c, &format!("INSERT INTO ik_src VALUES ({a}, {b}, {})", a * 100 + b)).await.unwrap();
        }
    }

    exec(&mut c, "BEGIN").await.unwrap();
    let locked = keys(&mut c, "SELECT a, b, u FROM ik WHERE a = 1 FOR UPDATE")
        .await
        .unwrap_or_else(|e| panic!("FOR UPDATE over rows whose u is NULL: {e}"));
    assert_eq!(locked.len(), 4);
    exec(&mut c, "COMMIT").await.unwrap();

    let n = exec(&mut c, "UPDATE ik SET n = ik_src.v FROM ik_src WHERE ik.a = ik_src.a AND ik.b = ik_src.b")
        .await
        .unwrap_or_else(|e| panic!("UPDATE … FROM over rows whose u is NULL: {e}"));
    assert_eq!(n, 12);
    let stored = sqlx::query("SELECT a, b, n FROM ik").fetch_all(&mut c).await.unwrap();
    assert_eq!(stored.len(), 12);
    for r in &stored {
        let (a, b, n) = (r.get::<i64, _>(0), r.get::<i64, _>(1), r.get::<i32, _>(2));
        assert_eq!(n as i64, a * 100 + b, "({a}, {b})");
    }
    let n = exec(&mut c, "DELETE FROM ik USING ik_src WHERE ik.a = ik_src.a AND ik.b = ik_src.b AND ik_src.b = 2")
        .await
        .unwrap_or_else(|e| panic!("DELETE … USING over rows whose u is NULL: {e}"));
    assert_eq!(n, 3);
    assert_eq!(keys(&mut c, "SELECT a, b FROM ik").await.unwrap().len(), 9);

    // Only a nullable key: its non-NULL rows are named; a NULL row is refused,
    // never written back by the bytes a NULL travels as.
    exec(&mut c, "CREATE TABLE ik_null (u BIGINT UNIQUE, n INT)").await.unwrap();
    exec(&mut c, "INSERT INTO ik_null (u, n) VALUES (1, 0), (NULL, 0), (0, 0), (NULL, 0)").await.unwrap();
    exec(&mut c, "CREATE TABLE ik_null_src (v INT)").await.unwrap();
    exec(&mut c, "INSERT INTO ik_null_src VALUES (5)").await.unwrap();
    exec(&mut c, "BEGIN").await.unwrap();
    let e = sqlx::query("SELECT u FROM ik_null WHERE n = 0 FOR UPDATE")
        .fetch_all(&mut c)
        .await
        .expect_err("FOR UPDATE named a NULL row by its key");
    assert!(e.to_string().contains("is NULL"), "{e}");
    exec(&mut c, "ROLLBACK").await.unwrap();
    let e = exec(&mut c, "UPDATE ik_null SET n = ik_null_src.v FROM ik_null_src WHERE ik_null.n = 0")
        .await
        .expect_err("UPDATE … FROM wrote back a NULL row by its key");
    assert!(e.to_string().contains("is NULL"), "{e}");
    let stored = sqlx::query("SELECT n FROM ik_null").fetch_all(&mut c).await.unwrap();
    assert!(stored.iter().all(|r| r.get::<i32, _>(0) == 0), "a refused statement wrote rows");
    let n = exec(&mut c, "UPDATE ik_null SET n = ik_null_src.v FROM ik_null_src WHERE ik_null.u = 0")
        .await
        .expect("a non-NULL key is named");
    assert_eq!(n, 1);
    let stored: BTreeSet<(Option<i64>, i32)> = sqlx::query("SELECT u, n FROM ik_null")
        .fetch_all(&mut c)
        .await
        .unwrap()
        .iter()
        .map(|r| (r.get::<Option<i64>, _>(0), r.get::<i32, _>(1)))
        .collect();
    assert_eq!(stored, BTreeSet::from([(None, 0), (Some(0), 5), (Some(1), 0)]));
}

/// `UPDATE … FROM` reads every relation, and every partition of each, under
/// one snapshot. A concurrent writer bumps every row of the source's `v` in
/// one transaction, so any one snapshot sees one `v` everywhere; `SET seen =
/// src.v` copies what the join saw into every target row, and a join that
/// read partitions under different snapshots stores (and returns) two values.
/// The writer touches only the source, so the write-back never conflicts
/// with it.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn update_from_reads_every_partition_under_one_snapshot() {
    let dir = root("snapshot");
    let server = TestServer::start_file_backed(&dir.0).await.expect("server");
    let mut setup = connect(&server).await;
    const TS: i64 = 32;
    const PER: i64 = 4;
    for (kind, partitioning) in [("plain", ""), ("part", " PARTITION BY HASH (tenant_id) PARTITIONS 32")] {
        let (t, s) = (format!("snap_{kind}"), format!("snap_src_{kind}"));
        for (name, extra) in [(&t, "seen BIGINT"), (&s, "v BIGINT NOT NULL")] {
            exec(
                &mut setup,
                &format!(
                    "CREATE TABLE {name} (tenant_id BIGINT NOT NULL, id BIGINT NOT NULL, {extra}, \
                     PRIMARY KEY (tenant_id, id)){partitioning}"
                ),
            )
            .await
            .unwrap();
        }
        for tenant in 1..=TS {
            let values: Vec<String> = (1..=PER).map(|id| format!("({tenant}, {id})")).collect();
            exec(&mut setup, &format!("INSERT INTO {t} (tenant_id, id) VALUES {}", values.join(", "))).await.unwrap();
            let values: Vec<String> = (1..=PER).map(|id| format!("({tenant}, {id}, 0)")).collect();
            exec(&mut setup, &format!("INSERT INTO {s} (tenant_id, id, v) VALUES {}", values.join(", "))).await.unwrap();
        }

        let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let writer = {
            let mut w = connect(&server).await;
            let (s, stop) = (s.clone(), stop.clone());
            tokio::spawn(async move {
                let mut bumps = 0u64;
                while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                    if exec(&mut w, &format!("UPDATE {s} SET v = v + 1")).await.is_ok() {
                        bumps += 1;
                    }
                }
                bumps
            })
        };

        // A violation stops the writer before anything asserts: a panic with
        // the writer still running left the test's runtime unable to shut
        // down, a hang rather than a failure.
        let mut r = connect(&server).await;
        let (mut ok, mut failed) = (0, 0);
        let mut values: BTreeSet<i64> = BTreeSet::new();
        let mut violation: Option<String> = None;
        let deadline = Instant::now() + Duration::from_secs(60);
        while ok < 40 && violation.is_none() && Instant::now() < deadline {
            let attempt = sqlx::query(AssertSqlSafe(format!(
                "UPDATE {t} SET seen = {s}.v FROM {s} WHERE {t}.tenant_id = {s}.tenant_id \
                 AND {t}.id = {s}.id RETURNING {t}.tenant_id, {t}.id, {t}.seen"
            )))
            .fetch_all(&mut r)
            .await;
            let rows = match attempt {
                Ok(rows) => rows,
                Err(e) => {
                    eprintln!("{t}: {e}");
                    failed += 1;
                    continue;
                }
            };
            let returned: BTreeMap<(i64, i64), i64> =
                rows.iter().map(|x| ((x.get::<i64, _>(0), x.get::<i64, _>(1)), x.get::<i64, _>(2))).collect();
            let seen: BTreeSet<i64> = returned.values().copied().collect();
            let stored: BTreeSet<Option<i64>> = sqlx::query(AssertSqlSafe(format!("SELECT seen FROM {t}")))
                .fetch_all(&mut r)
                .await
                .unwrap()
                .iter()
                .map(|x| x.get::<Option<i64>, _>(0))
                .collect();
            if rows.len() as i64 != TS * PER || returned.len() as i64 != TS * PER {
                violation = Some(format!("RETURNING held {} rows, {} distinct", rows.len(), returned.len()));
            } else if seen.len() != 1 {
                violation = Some(format!("the join saw the writer's transaction half applied: {seen:?}"));
            } else if stored != seen.iter().map(|v| Some(*v)).collect() {
                violation = Some(format!("stored {stored:?} differs from RETURNING {seen:?}"));
            }
            values.extend(seen);
            ok += 1;
        }
        stop.store(true, std::sync::atomic::Ordering::Relaxed);
        let bumps = tokio::time::timeout(Duration::from_secs(30), writer).await.expect("the writer did not stop").expect("writer");
        assert!(violation.is_none(), "{t}: {}", violation.unwrap_or_default());
        assert!(ok >= 20 && failed <= ok, "{t}: {ok} of {} UPDATE … FROM succeeded under the writer", ok + failed);
        // The writer committed during the reads, or nothing was tested.
        assert!(bumps > 0 && values.len() > 5, "{t}: {bumps} bumps, values seen {values:?}");
    }
}

/// `DELETE … USING` under a concurrent writer that inserts target and source
/// rows together: every inserted key is returned by exactly one delete or is
/// still in the table, and each delete's count is its `RETURNING`'s length.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn delete_using_returns_each_row_once_under_a_concurrent_writer() {
    let dir = root("delete");
    let server = TestServer::start_file_backed(&dir.0).await.expect("server");
    let mut setup = connect(&server).await;
    for (kind, partitioning) in KINDS {
        let (t, s) = (format!("del_{kind}"), format!("del_src_{kind}"));
        exec(
            &mut setup,
            &format!("CREATE TABLE {t} (tenant_id BIGINT NOT NULL, id BIGINT NOT NULL, PRIMARY KEY (tenant_id, id)){partitioning}"),
        )
        .await
        .unwrap();
        exec(&mut setup, &format!("CREATE TABLE {s} (tenant_id BIGINT NOT NULL, id BIGINT NOT NULL)")).await.unwrap();

        const BATCHES: i64 = 40;
        const PER: i64 = 10;
        let writer = {
            let mut w = connect(&server).await;
            let (t, s) = (t.clone(), s.clone());
            tokio::spawn(async move {
                for batch in 0..BATCHES {
                    let values: Vec<String> =
                        (0..PER).map(|i| format!("({}, {})", i % TENANTS + 1, batch * PER + i)).collect();
                    exec(&mut w, "BEGIN").await.unwrap();
                    exec(&mut w, &format!("INSERT INTO {t} VALUES {}", values.join(", "))).await.unwrap();
                    exec(&mut w, &format!("INSERT INTO {s} VALUES {}", values.join(", "))).await.unwrap();
                    exec(&mut w, "COMMIT").await.unwrap();
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            })
        };
        let mut d = connect(&server).await;
        let mut returned: BTreeMap<(i64, i64), usize> = BTreeMap::new();
        let delete = format!(
            "DELETE FROM {t} USING {s} WHERE {t}.tenant_id = {s}.tenant_id AND {t}.id = {s}.id \
             RETURNING {t}.tenant_id, {t}.id"
        );
        let deadline = Instant::now() + Duration::from_secs(60);
        let mut finishing = 0;
        while finishing < 2 {
            assert!(Instant::now() < deadline, "{t}: did not finish in time");
            if writer.is_finished() {
                finishing += 1;
            }
            if let Ok(got) = keys(&mut d, &delete).await {
                for k in got {
                    *returned.entry(k).or_default() += 1;
                }
            }
        }
        writer.await.expect("writer");
        let twice: Vec<_> = returned.iter().filter(|(_, n)| **n > 1).collect();
        assert!(twice.is_empty(), "{t}: rows returned by two deletes: {twice:?}");
        let left: BTreeSet<(i64, i64)> = keys(&mut d, &format!("SELECT tenant_id, id FROM {t}")).await.unwrap().into_iter().collect();
        assert!(left.is_empty(), "{t}: the final delete left {left:?}");
        assert_eq!(returned.len() as i64, BATCHES * PER, "{t}: every inserted row returned once");
        let n = exec(&mut d, &format!("DELETE FROM {t} USING {s} WHERE {t}.tenant_id = {s}.tenant_id AND {t}.id = {s}.id"))
            .await
            .unwrap();
        assert_eq!(n, 0, "{t}");
    }
}

/// The value table of the lost-update repros: `(tag, k)` keyed, `v = 0`,
/// and a source giving tag 1 the weight 100.
async fn lost_update_tables(conn: &mut PgConnection, kind: &str, partitioning: &str) -> (String, String) {
    let (t, s) = (format!("ft_{kind}"), format!("fsrc_{kind}"));
    let partitioning = partitioning.replace("tenant_id", "tag");
    exec(
        conn,
        &format!("CREATE TABLE {t} (tag BIGINT NOT NULL, k BIGINT NOT NULL, v BIGINT NOT NULL, PRIMARY KEY (tag, k)){partitioning}"),
    )
    .await
    .unwrap();
    exec(conn, &format!("INSERT INTO {t} (tag, k, v) VALUES (1, 1, 0)")).await.unwrap();
    exec(conn, &format!("CREATE TABLE {s} (tag BIGINT NOT NULL, w BIGINT NOT NULL)")).await.unwrap();
    exec(conn, &format!("INSERT INTO {s} (tag, w) VALUES (1, 100)")).await.unwrap();
    (t, s)
}

async fn value_of(conn: &mut PgConnection, t: &str) -> i64 {
    sqlx::query(AssertSqlSafe(format!("SELECT v FROM {t} WHERE tag = 1 AND k = 1")))
        .fetch_one(&mut *conn)
        .await
        .expect("read back")
        .get::<i64, _>(0)
}

/// The holder of a row lock sets `v = 5` and commits `after` into the
/// writer's run (or as soon as the writer has finished, if sooner); returns
/// the writer's outcome.
async fn write_against_a_holder(
    server: &TestServer,
    t: &str,
    writer: String,
    after: Duration,
) -> Result<u64, sqlx::Error> {
    let mut a = connect(server).await;
    exec(&mut a, "BEGIN").await.unwrap();
    keys(&mut a, &format!("SELECT tag, k FROM {t} WHERE tag = 1 AND k = 1 FOR UPDATE")).await.expect("lock");
    let mut b = connect(server).await;
    let mut hb = tokio::spawn(async move { exec(&mut b, &writer).await });
    let early = tokio::time::timeout(after, &mut hb).await;
    exec(&mut a, &format!("UPDATE {t} SET v = 5 WHERE tag = 1 AND k = 1")).await.expect("the holder's write");
    exec(&mut a, "COMMIT").await.expect("commit");
    match early {
        Ok(joined) => joined.expect("join"),
        Err(_) => tokio::time::timeout(Duration::from_secs(30), hb).await.expect("the writer hung").expect("join"),
    }
}

/// An `UPDATE … FROM` meeting a row another transaction holds never writes
/// the values it computed from its join over the holder's commit: it fails,
/// or it writes `v + w` over the holder's `v`. It used to wait for the
/// holder, then write 0 + 100 over the holder's 5 and report success.
#[tokio::test(flavor = "multi_thread")]
async fn update_from_never_writes_over_a_holders_commit() {
    let dir = root("lost_update");
    let server = TestServer::start_file_backed(&dir.0).await.expect("server");
    let mut setup = connect(&server).await;
    for (kind, partitioning) in KINDS {
        let (t, s) = lost_update_tables(&mut setup, kind, partitioning).await;
        let writer = format!("UPDATE {t} AS t SET v = t.v + s.w FROM {s} s WHERE t.tag = s.tag");
        let outcome = write_against_a_holder(&server, &t, writer, Duration::from_secs(3)).await;
        let v = value_of(&mut setup, &t).await;
        match outcome {
            Err(e) => {
                assert!(e.to_string().contains("is locked by transaction"), "{t}: {e}");
                assert_eq!(v, 5, "{t}: the refused writer changed the row");
            }
            Ok(n) => assert_eq!((n, v), (1, 105), "{t}: the writer's join values landed over the holder's commit"),
        }
    }
}

/// The per-row recheck of `UPDATE … FROM` (`… AND t.v = 0`) is never
/// dropped because the row was locked: the write that followed a lock
/// timeout used to retry by key alone, without it, and land once the holder
/// had committed `v = 5` (`UPDATE 1`, v = 100, where 0 rows and v = 5 were
/// right).
#[tokio::test(flavor = "multi_thread")]
async fn update_from_keeps_its_recheck_when_the_row_is_locked() {
    let dir = root("recheck_kept");
    let server = TestServer::start_file_backed(&dir.0).await.expect("server");
    let mut setup = connect(&server).await;
    for (kind, partitioning) in KINDS {
        let (t, s) = lost_update_tables(&mut setup, kind, partitioning).await;
        let writer = format!("UPDATE {t} AS t SET v = t.v + s.w FROM {s} s WHERE t.tag = s.tag AND t.v = 0");
        let outcome = write_against_a_holder(&server, &t, writer, Duration::from_secs(13)).await;
        let v = value_of(&mut setup, &t).await;
        match outcome {
            Err(e) => assert!(e.to_string().contains("is locked by transaction"), "{t}: {e}"),
            Ok(n) => assert_eq!(n, 0, "{t}: a write landed without its recheck"),
        }
        assert_eq!(v, 5, "{t}");
    }
}

/// Every key type locks one row however the statement reached it: the
/// locker's cells come from the select pipeline at the column's declared
/// scale and the writer's through a subquery at the engine's, and a
/// `NUMERIC(10,2)` key used to differ between the two (150 and 1500000000),
/// so the writer never met the lock. Select lists name the key columns out
/// of their declared order, or not at all.
#[tokio::test(flavor = "multi_thread")]
async fn every_key_type_is_locked_and_checked_by_the_same_bytes() {
    let dir = root("key_types");
    let server = TestServer::start_file_backed(&dir.0).await.expect("server");
    let mut setup = connect(&server).await;
    let u1 = "6f1c0d6e-9a1b-4a55-8a0b-2d7f3c1e9a01";
    let u2 = "6f1c0d6e-9a1b-4a55-8a0b-2d7f3c1e9a02";
    // (table, create, rows, select list, held key, other key)
    let cases: Vec<(&str, String, String, &str, String, String)> = vec![
        (
            "num_one",
            "CREATE TABLE num_one (amt NUMERIC(10,2) PRIMARY KEY, n INT)".into(),
            "INSERT INTO num_one (amt, n) VALUES (1.50, 0), (2.25, 0)".into(),
            "n, amt",
            "amt = 1.50".into(),
            "amt = 2.25".into(),
        ),
        (
            "num_two",
            "CREATE TABLE num_two (tag BIGINT NOT NULL, amt NUMERIC(10,2) NOT NULL, n INT, PRIMARY KEY (tag, amt))".into(),
            "INSERT INTO num_two (tag, amt, n) VALUES (1, 1.50, 0), (1, 2.25, 0)".into(),
            "amt, n, tag",
            "tag = 1 AND amt = 1.50".into(),
            "tag = 1 AND amt = 2.25".into(),
        ),
        (
            "num_part",
            "CREATE TABLE num_part (tag BIGINT NOT NULL, amt NUMERIC(10,2) NOT NULL, n INT, PRIMARY KEY (tag, amt)) \
             PARTITION BY HASH (tag) PARTITIONS 4"
                .into(),
            "INSERT INTO num_part (tag, amt, n) VALUES (1, 1.50, 0), (1, 2.25, 0)".into(),
            "n",
            "tag = 1 AND amt = 1.50".into(),
            "tag = 1 AND amt = 2.25".into(),
        ),
        (
            "num_nine",
            "CREATE TABLE num_nine (amt NUMERIC(10,9) PRIMARY KEY, n INT)".into(),
            "INSERT INTO num_nine (amt, n) VALUES (1.5, 0), (2.25, 0)".into(),
            "amt, n",
            "amt = 1.5".into(),
            "amt = 2.25".into(),
        ),
        (
            "txt",
            "CREATE TABLE txt (tag BIGINT NOT NULL, name TEXT NOT NULL, n INT, PRIMARY KEY (tag, name))".into(),
            "INSERT INTO txt (tag, name, n) VALUES (1, 'it''s', 0), (1, 'b', 0)".into(),
            "n, name, tag",
            "tag = 1 AND name = 'it''s'".into(),
            "tag = 1 AND name = 'b'".into(),
        ),
        (
            "uid",
            "CREATE TABLE uid (id UUID NOT NULL PRIMARY KEY, n INT)".into(),
            format!("INSERT INTO uid (id, n) VALUES ('{u1}', 0), ('{u2}', 0)"),
            "n, id",
            format!("id = '{u1}'"),
            format!("id = '{u2}'"),
        ),
    ];
    for (t, create, rows, list, held, other) in cases {
        exec(&mut setup, &create).await.unwrap_or_else(|e| panic!("{t}: {e}"));
        exec(&mut setup, &rows).await.unwrap_or_else(|e| panic!("{t}: {e}"));
        let mut a = connect(&server).await;
        exec(&mut a, "BEGIN").await.unwrap();
        let got = sqlx::query(AssertSqlSafe(format!("SELECT {list} FROM {t} WHERE {held} FOR UPDATE")))
            .fetch_all(&mut a)
            .await
            .unwrap_or_else(|e| panic!("{t}: lock: {e}"));
        assert_eq!(got.len(), 1, "{t}");
        assert_eq!(got[0].len(), list.split(',').count(), "{t}: the key fetch reached the client");

        let mut b = connect(&server).await;
        let e = exec(&mut b, &format!("UPDATE {t} SET n = 9 WHERE {held}"))
            .await
            .expect_err(&format!("{t}: a writer ran past the held row"));
        assert!(e.to_string().contains("is locked by transaction"), "{t}: {e}");
        exec(&mut b, "BEGIN").await.unwrap();
        let e = exec(&mut b, &format!("SELECT {list} FROM {t} WHERE {held} FOR UPDATE NOWAIT"))
            .await
            .expect_err(&format!("{t}: NOWAIT on the held row succeeded"));
        assert_eq!(code(&e).as_deref(), Some("55P03"), "{t}: {e}");
        exec(&mut b, "ROLLBACK").await.unwrap();
        assert_eq!(exec(&mut b, &format!("UPDATE {t} SET n = 3 WHERE {other}")).await.expect("other row"), 1, "{t}");

        exec(&mut a, "COMMIT").await.unwrap();
        assert_eq!(exec(&mut b, &format!("UPDATE {t} SET n = 9 WHERE {held}")).await.expect("released"), 1, "{t}");
        let ns: BTreeSet<i32> = sqlx::query(AssertSqlSafe(format!("SELECT n FROM {t}")))
            .fetch_all(&mut b)
            .await
            .unwrap()
            .iter()
            .map(|r| r.get::<i32, _>(0))
            .collect();
        assert_eq!(ns, BTreeSet::from([3, 9]), "{t}");
    }
}

/// A locking read need not select the key: the table keeps one identifying
/// key (here the primary key `id`, not the inline UNIQUE `email`; for the
/// partitioned queue, both key columns), and the scan fetches it for the
/// lock without returning it. A nullable key fetched that way still refuses
/// a NULL row rather than keying every NULL row alike.
#[tokio::test(flavor = "multi_thread")]
async fn a_locking_read_need_not_select_the_key() {
    let dir = root("hidden_key");
    let server = TestServer::start_file_backed(&dir.0).await.expect("server");
    let mut setup = connect(&server).await;
    exec(&mut setup, "CREATE TABLE hk (id BIGINT PRIMARY KEY, email TEXT UNIQUE, n INT)").await.unwrap();
    exec(&mut setup, "INSERT INTO hk (id, email, n) VALUES (1, 'a', 0), (2, 'b', 0)").await.unwrap();
    let q = queue(&mut setup, "part", KINDS[1].1).await;
    for (t, read, held_write, other_write) in [
        ("hk", "SELECT email FROM hk WHERE email = 'a' FOR UPDATE", "UPDATE hk SET n = 1 WHERE id = 1", "UPDATE hk SET n = 1 WHERE id = 2"),
        (
            q.as_str(),
            "SELECT state FROM q_part WHERE tenant_id = 2 AND id = 3 FOR UPDATE",
            "UPDATE q_part SET claimed_by = 1 WHERE tenant_id = 2 AND id = 3",
            "UPDATE q_part SET claimed_by = 1 WHERE tenant_id = 2 AND id = 4",
        ),
    ] {
        let mut a = connect(&server).await;
        exec(&mut a, "BEGIN").await.unwrap();
        let got = sqlx::query(AssertSqlSafe(read.to_string()))
            .fetch_all(&mut a)
            .await
            .unwrap_or_else(|e| panic!("{t}: a locking read without the key: {e}"));
        assert_eq!(got.len(), 1, "{t}");
        assert_eq!(got[0].len(), 1, "{t}: the key fetch reached the client");
        let mut b = connect(&server).await;
        let e = exec(&mut b, held_write).await.expect_err(&format!("{t}: the row was not locked"));
        assert!(e.to_string().contains("is locked by transaction"), "{t}: {e}");
        assert_eq!(exec(&mut b, other_write).await.expect("other row"), 1, "{t}");
        exec(&mut a, "COMMIT").await.unwrap();
        assert_eq!(exec(&mut b, held_write).await.expect("released"), 1, "{t}");
    }

    exec(&mut setup, "CREATE TABLE hk_null (u BIGINT UNIQUE, n INT)").await.unwrap();
    exec(&mut setup, "INSERT INTO hk_null (u, n) VALUES (1, 0), (NULL, 1), (NULL, 2)").await.unwrap();
    exec(&mut setup, "BEGIN").await.unwrap();
    let e = exec(&mut setup, "SELECT n FROM hk_null WHERE n = 1 FOR UPDATE")
        .await
        .expect_err("a NULL row was keyed by its key");
    assert!(e.to_string().contains("is NULL"), "{e}");
    exec(&mut setup, "ROLLBACK").await.unwrap();
    exec(&mut setup, "BEGIN").await.unwrap();
    let got = sqlx::query("SELECT n FROM hk_null WHERE n = 0 FOR UPDATE").fetch_all(&mut setup).await.expect("non-NULL");
    assert_eq!(got.len(), 1);
    exec(&mut setup, "COMMIT").await.unwrap();
}

/// A locking read that waited for a holder returns what the holder left:
/// a row the holder changed so it no longer matches is left out, and a row
/// it changed otherwise comes back as changed. It used to come back as the
/// waiter's scan saw it, handing a claim query a job already done.
#[tokio::test(flavor = "multi_thread")]
async fn a_locking_read_that_waited_returns_the_rows_as_the_holder_left_them() {
    let dir = root("waited");
    let server = TestServer::start_file_backed(&dir.0).await.expect("server");
    let mut setup = connect(&server).await;
    for (kind, partitioning) in KINDS {
        let t = queue(&mut setup, kind, partitioning).await;
        let mut a = connect(&server).await;
        exec(&mut a, "BEGIN").await.unwrap();
        keys(&mut a, &format!("SELECT tenant_id, id FROM {t} WHERE tenant_id = 1 AND id <= 2 FOR UPDATE"))
            .await
            .expect("lock");
        let mut b = connect(&server).await;
        let t2 = t.clone();
        let hb = tokio::spawn(async move {
            exec(&mut b, "BEGIN").await.unwrap();
            let rows = sqlx::query(AssertSqlSafe(format!(
                "SELECT id, state, claimed_by, tenant_id FROM {t2} WHERE tenant_id = 1 AND id <= 3 AND state = 'new' \
                 FOR UPDATE"
            )))
            .fetch_all(&mut b)
            .await;
            let _ = exec(&mut b, "COMMIT").await;
            rows.map(|rows| {
                rows.iter()
                    .map(|r| (r.get::<i64, _>(0), r.get::<String, _>(1), r.get::<Option<i64>, _>(2)))
                    .collect::<Vec<_>>()
            })
        });
        tokio::time::sleep(Duration::from_millis(400)).await;
        assert!(!hb.is_finished(), "{t}: the locking read did not wait for the holder");
        exec(&mut a, &format!("UPDATE {t} SET state = 'done' WHERE tenant_id = 1 AND id = 1")).await.unwrap();
        exec(&mut a, &format!("UPDATE {t} SET claimed_by = 7 WHERE tenant_id = 1 AND id = 2")).await.unwrap();
        exec(&mut a, "COMMIT").await.unwrap();
        let mut got = tokio::time::timeout(Duration::from_secs(20), hb)
            .await
            .expect("the waiter hung")
            .expect("join")
            .unwrap_or_else(|e| panic!("{t}: {e}"));
        got.sort();
        assert_eq!(
            got,
            vec![(2, "new".to_string(), Some(7)), (3, "new".to_string(), None)],
            "{t}: the waiter returned rows as its scan saw them"
        );
    }
}
