//! An UPDATE or a DELETE reads its subqueries and its target rows as of one
//! moment, as PostgreSQL reads a statement under READ COMMITTED.
//!
//! `t` holds exactly one row, id `k`, and `u` exactly one, `(1, k, 3 - k)`:
//! its `tid` names the `t` row and its `other` names the id `t` does not
//! hold. A writer moves the pair between `k = 1` and `k = 2`, both rows in
//! one transaction. So under any one snapshot
//!
//! * `NOT EXISTS (… u.tid = t.id)` and `id NOT IN (SELECT tid FROM u)` hold
//!   for no `t` row, and
//! * `EXISTS (… u.other = t.id)`, `id IN (SELECT other FROM u)` and `id =
//!   (SELECT other …)` hold for no `t` row either,
//!
//! whether the subquery sits in the predicate, beside a `FROM`/`USING`
//! source, or reads a CTE, and a derived source joined on `other` matches
//! nothing either: every statement below must touch nothing. A statement whose subquery
//! is read under one snapshot and whose target is scanned under a later one
//! sees `k = 1` in `u` and `k = 2` in `t` when a move commits in between,
//! and writes the row it must not. The writer moves the pair for as long as
//! the reader runs its rounds: no sleeps, nothing timed.

use rasterizeddb_compat::TestServer;
use sqlx::{AssertSqlSafe, Executor, Row};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

/// Rounds of the four forms per test. The old code, which read the two
/// halves under two snapshots, wrote a wrong row within the first two.
const ROUNDS: usize = 20;

/// Filler in `u` outside both keys, so the subquery's scan takes long
/// enough for a move to commit during it.
const FILLER: i64 = 2_000;

/// Each form as an `UPDATE` and as a `DELETE`, every one touching nothing
/// under any single snapshot.
const FORMS: [(&str, &str, &str); 8] = [
    (
        "NOT EXISTS",
        "UPDATE t SET v = v + 1 WHERE NOT EXISTS (SELECT 1 FROM u WHERE u.tid = t.id) RETURNING id",
        "DELETE FROM t WHERE NOT EXISTS (SELECT 1 FROM u WHERE u.tid = t.id) RETURNING id",
    ),
    (
        "EXISTS",
        "UPDATE t SET v = v + 1 WHERE EXISTS (SELECT 1 FROM u WHERE u.other = t.id) RETURNING id",
        "DELETE FROM t WHERE EXISTS (SELECT 1 FROM u WHERE u.other = t.id) RETURNING id",
    ),
    (
        "IN",
        "UPDATE t SET v = v + 1 WHERE id IN (SELECT other FROM u) RETURNING id",
        "DELETE FROM t WHERE id IN (SELECT other FROM u) RETURNING id",
    ),
    (
        "NOT IN",
        "UPDATE t SET v = v + 1 WHERE id NOT IN (SELECT tid FROM u WHERE tid IS NOT NULL) RETURNING id",
        "DELETE FROM t WHERE id NOT IN (SELECT tid FROM u WHERE tid IS NOT NULL) RETURNING id",
    ),
    (
        "scalar",
        "UPDATE t SET v = v + 1 WHERE id = (SELECT other FROM u WHERE id = 1) RETURNING id",
        "DELETE FROM t WHERE id = (SELECT other FROM u WHERE id = 1) RETURNING id",
    ),
    (
        "FROM/USING with a subquery",
        "UPDATE t SET v = t.v + 1 FROM u WHERE u.id = 1 AND t.id NOT IN (SELECT tid FROM u WHERE tid IS NOT NULL) RETURNING t.id",
        "DELETE FROM t USING u WHERE u.id = 1 AND t.id NOT IN (SELECT tid FROM u WHERE tid IS NOT NULL) RETURNING t.id",
    ),
    (
        "CTE",
        "WITH p AS (SELECT tid FROM u WHERE tid IS NOT NULL) UPDATE t SET v = v + 1 WHERE id NOT IN (SELECT tid FROM p) RETURNING id",
        "WITH p AS (SELECT tid FROM u WHERE tid IS NOT NULL) DELETE FROM t WHERE id NOT IN (SELECT tid FROM p) RETURNING id",
    ),
    (
        "derived source",
        "UPDATE t SET v = t.v + 1 FROM (SELECT other FROM u) p WHERE p.other = t.id RETURNING t.id",
        "DELETE FROM t USING (SELECT other FROM u) p WHERE p.other = t.id RETURNING t.id",
    ),
];

#[derive(Clone, Copy, PartialEq)]
enum Verb {
    Update,
    Delete,
}

#[derive(Clone, Copy, PartialEq)]
enum Mode {
    Autocommit,
    ReadCommittedBlock,
    /// Autocommit under a session's `REPEATABLE READ` characteristics.
    SessionRepeatableRead,
}

async fn setup(pool: &sqlx::PgPool) {
    pool.execute(sqlx::raw_sql(
        "CREATE TABLE t (id INT PRIMARY KEY, v INT NOT NULL);
         CREATE TABLE u (id INT PRIMARY KEY, tid INT, other INT);
         INSERT INTO t VALUES (1, 0);
         INSERT INTO u VALUES (1, 1, 2);",
    ))
    .await
    .expect("schema");
    for batch in 0..(FILLER / 500) {
        let values: Vec<String> = (0..500i64)
            .map(|i| {
                let id = 1_000 + batch * 500 + i;
                format!("({id}, {id}, {id})")
            })
            .collect();
        pool.execute(AssertSqlSafe(format!("INSERT INTO u VALUES {}", values.join(", "))))
            .await
            .expect("filler");
    }
}

/// Moves the pair until `stop`, each move one transaction, retried whole
/// when it meets a reader's lock. Counts the moves made in `moves`.
fn mover(pool: &sqlx::PgPool, stop: Arc<AtomicBool>, moves: Arc<AtomicUsize>) -> tokio::task::JoinHandle<()> {
    let pool = pool.clone();
    tokio::spawn(async move {
        for i in 0.. {
            if stop.load(Ordering::Relaxed) {
                break;
            }
            let (from, to) = if i % 2 == 0 { (1, 2) } else { (2, 1) };
            let mut attempts = 0;
            loop {
                attempts += 1;
                assert!(attempts <= 200, "move {i} failed 200 times");
                let mut tx = pool.begin().await.expect("begin");
                let moved = async {
                    tx.execute(AssertSqlSafe(format!("DELETE FROM t WHERE id = {from}"))).await?;
                    tx.execute("DELETE FROM u WHERE id = 1").await?;
                    tx.execute(AssertSqlSafe(format!("INSERT INTO t VALUES ({to}, 0)"))).await?;
                    tx.execute(AssertSqlSafe(format!("INSERT INTO u VALUES (1, {to}, {})", 3 - to)))
                        .await?;
                    Ok::<_, sqlx::Error>(())
                }
                .await;
                match moved {
                    Ok(()) => {
                        tx.commit().await.expect("commit");
                        moves.fetch_add(1, Ordering::Relaxed);
                        break;
                    }
                    Err(_) => {
                        let _ = tx.rollback().await;
                    }
                }
            }
        }
    })
}

/// Runs every form `ROUNDS` times against the moving pair, and fails naming
/// each form that touched a row.
async fn touches_nothing(verb: Verb, mode: Mode) {
    let server = TestServer::start().await.expect("server");
    let pool = server.connect_sqlx().await.expect("connect");
    setup(&pool).await;
    let mut reader = pool.acquire().await.expect("reader");
    if mode == Mode::SessionRepeatableRead {
        reader
            .execute("SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            .await
            .expect("session characteristics");
    }
    let stop = Arc::new(AtomicBool::new(false));
    let moves = Arc::new(AtomicUsize::new(0));
    let mover = mover(&pool, stop.clone(), moves.clone());

    // Per form: statements run, statements refused, rows wrongly touched.
    let mut runs = [0u64; FORMS.len()];
    let mut errors = [0u64; FORMS.len()];
    let mut touched: [Vec<i32>; FORMS.len()] = Default::default();
    let mut elapsed = [Duration::ZERO; FORMS.len()];
    let mut first_error: Option<String> = None;
    let deadline = Instant::now() + Duration::from_secs(180);
    for _ in 0..ROUNDS {
        assert!(Instant::now() < deadline, "the rounds did not finish in time");
        assert!(!mover.is_finished(), "the mover stopped early");
        for (form, (_, update, delete)) in FORMS.iter().enumerate() {
            let sql = if verb == Verb::Update { *update } else { *delete };
            if mode == Mode::ReadCommittedBlock {
                reader.execute("BEGIN ISOLATION LEVEL READ COMMITTED").await.expect("begin");
            }
            let started = Instant::now();
            let result = reader.fetch_all(sqlx::raw_sql(sql)).await;
            elapsed[form] += started.elapsed();
            runs[form] += 1;
            match result {
                Ok(rows) => touched[form].extend(rows.iter().map(|r| r.get::<i32, _>(0))),
                Err(e) => {
                    errors[form] += 1;
                    first_error.get_or_insert_with(|| format!("{sql}: {e}"));
                }
            }
            if mode == Mode::ReadCommittedBlock {
                // COMMIT of a failed block rolls it back, as in PostgreSQL.
                reader.execute("COMMIT").await.expect("commit");
            }
        }
    }
    stop.store(true, Ordering::Relaxed);
    mover.await.expect("mover");
    let moves = moves.load(Ordering::Relaxed);

    let report: Vec<String> = FORMS
        .iter()
        .enumerate()
        .map(|(i, (name, _, _))| {
            format!(
                "{name}: {} rows touched, {} errors in {} statements ({:?})",
                touched[i].len(),
                errors[i],
                runs[i],
                elapsed[i]
            )
        })
        .chain(std::iter::once(format!("{moves} moves")))
        .collect();
    eprintln!("{}", report.join("; "));
    // Without moves during the rounds nothing was tested.
    assert!(moves >= ROUNDS, "too few moves to judge; {}", report.join("; "));
    for (i, (name, _, _)) in FORMS.iter().enumerate() {
        assert!(
            touched[i].is_empty(),
            "{name} touched rows it must not (ids {:?}); {}",
            &touched[i][..touched[i].len().min(5)],
            report.join("; ")
        );
        // A form refused every time would pass vacuously.
        assert!(
            errors[i] * 2 <= runs[i],
            "{name} ran too few times to judge; {}; first error: {first_error:?}",
            report.join("; ")
        );
    }

    // The pair is whole: no statement above removed or changed it.
    let k = if moves % 2 == 0 { 1 } else { 2 };
    let t: Vec<(i32, i32)> = pool
        .fetch_all("SELECT id, v FROM t")
        .await
        .expect("t")
        .iter()
        .map(|r| (r.get(0), r.get(1)))
        .collect();
    assert_eq!(t, vec![(k, 0)], "{}", report.join("; "));
}

#[tokio::test(flavor = "multi_thread")]
async fn an_update_reads_its_subqueries_with_its_rows_autocommit() {
    touches_nothing(Verb::Update, Mode::Autocommit).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_delete_reads_its_subqueries_with_its_rows_autocommit() {
    touches_nothing(Verb::Delete, Mode::Autocommit).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn an_update_reads_its_subqueries_with_its_rows_read_committed() {
    touches_nothing(Verb::Update, Mode::ReadCommittedBlock).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_delete_reads_its_subqueries_with_its_rows_read_committed() {
    touches_nothing(Verb::Delete, Mode::ReadCommittedBlock).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn an_update_reads_its_subqueries_with_its_rows_session_repeatable_read() {
    touches_nothing(Verb::Update, Mode::SessionRepeatableRead).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_delete_reads_its_subqueries_with_its_rows_session_repeatable_read() {
    touches_nothing(Verb::Delete, Mode::SessionRepeatableRead).await;
}
