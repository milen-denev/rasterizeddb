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

/// A `FOR UPDATE` holder blocks an `UPDATE` of the same row through another
/// session, routed or fanned out, until it commits; a row sharing its first
/// key column is written at once.
#[tokio::test(flavor = "multi_thread")]
async fn a_held_row_blocks_a_writer_until_commit() {
    let dir = root("blocks");
    let server = TestServer::start_file_backed(&dir.0).await.expect("server");
    let mut setup = connect(&server).await;
    for (kind, partitioning) in KINDS {
        let t = queue(&mut setup, kind, partitioning).await;
        for (what, writer) in [
            ("routed", format!("UPDATE {t} SET state = 'stolen' WHERE tenant_id = 1 AND id = 1 AND state = 'new'")),
            ("fanned out", format!("UPDATE {t} SET state = 'stolen' WHERE id = 1 AND state = 'new'")),
        ] {
            exec(&mut setup, &format!("UPDATE {t} SET state = 'new' WHERE id = 1")).await.unwrap();
            let mut a = connect(&server).await;
            exec(&mut a, "BEGIN").await.unwrap();
            keys(&mut a, &format!("SELECT tenant_id, id FROM {t} WHERE tenant_id = 1 AND id = 1 FOR UPDATE"))
                .await
                .unwrap_or_else(|e| panic!("{t}: lock: {e}"));

            let mut b = connect(&server).await;
            let started = Instant::now();
            let blocked = tokio::spawn(async move { (exec(&mut b, &writer).await, started.elapsed()) });

            // Another row of the same tenant is not held.
            let mut c = connect(&server).await;
            let quick = Instant::now();
            let n = exec(&mut c, &format!("UPDATE {t} SET claimed_by = 7 WHERE tenant_id = 1 AND id = 2"))
                .await
                .unwrap_or_else(|e| panic!("{t} {what}: a row sharing the held key's first column: {e}"));
            assert_eq!(n, 1, "{t} {what}");
            assert!(
                quick.elapsed() < Duration::from_secs(2),
                "{t} {what}: a row sharing the held key's first column waited {:?}",
                quick.elapsed()
            );

            tokio::time::sleep(Duration::from_millis(600)).await;
            assert!(!blocked.is_finished(), "{t} {what}: the writer ran past a row held FOR UPDATE");

            exec(&mut a, &format!("UPDATE {t} SET state = 'claimed' WHERE tenant_id = 1 AND id = 1"))
                .await
                .unwrap_or_else(|e| panic!("{t}: the holder's own write: {e}"));
            let held_for = Instant::now();
            exec(&mut a, "COMMIT").await.unwrap();
            let (outcome, waited) = tokio::time::timeout(Duration::from_secs(20), blocked)
                .await
                .expect("the writer never finished")
                .expect("join");
            let n = outcome.unwrap_or_else(|e| panic!("{t} {what}: the writer failed after the commit: {e}"));
            assert!(waited >= Duration::from_millis(600), "{t} {what}: finished after {waited:?}");
            assert!(held_for.elapsed() < Duration::from_secs(10), "{t} {what}");

            // The writer re-read the row after the holder committed: (1, 1) is
            // the holder's, and only the other tenants' rows were stolen.
            let after = states(&mut setup, &t).await;
            assert_eq!(after[&(1, 1)].0, "claimed", "{t} {what}: the writer overwrote the holder's claim");
            let stolen: Vec<(i64, i64)> = after.iter().filter(|(_, (s, _))| s == "stolen").map(|(k, _)| *k).collect();
            let want: Vec<(i64, i64)> =
                if what == "routed" { vec![] } else { (2..=TENANTS).map(|tenant| (tenant, 1)).collect() };
            assert_eq!(stolen, want, "{t} {what}");
            assert_eq!(n, want.len() as u64, "{t} {what}: the writer's count");
            assert_eq!(after[&(1, 2)].1, Some(7), "{t} {what}");
        }
    }
}

/// A cycle through a row lock and a table lock, either way round and with
/// either a locking read or a writer waiting on the row, ends in exactly one
/// 40P01 within a few seconds.
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
            format!("UPDATE {t} SET claimed_by = 1 WHERE tenant_id = 3 AND id = 4"),
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
/// k` and mark what they claimed, each in its own block. Every job is claimed
/// exactly once, and what is stored names the worker that claimed it.
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
                                     WHERE tenant_id = {tenant} AND id = {id} AND state = 'new'"
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
