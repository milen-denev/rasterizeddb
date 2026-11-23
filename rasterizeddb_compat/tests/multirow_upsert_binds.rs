//! `ON CONFLICT DO NOTHING` on a multi-row INSERT sent the way an ORM sends it.
//!
//! The engine cuts `INSERT … VALUES (…), (…)` into one statement per tuple and
//! re-parses each, so a clause the parser split off the parent is absent from
//! every row unless it is explicitly carried across. That cost `ON CONFLICT`
//! once already, and the fix is in `insert_values_batch`.
//!
//! What was never covered is the path an ORM actually uses: a **prepared
//! statement over the extended protocol** with `$N` bind parameters, rather
//! than literals inlined by `psql`. sqlx binds, so these tests exercise that
//! axis specifically — the placeholders are substituted somewhere before the
//! tuples are cut, and whether the clause survives that is a different question
//! from whether it survives the simple protocol.
//!
//! The shape is PMS's: an idempotent page copy where some rows already exist.
//! Its whole purpose is "insert what fits and skip the rest", so a raised
//! duplicate-key error is not a smaller failure than a wrong row — the batch
//! loop stops on the first error, and the rows that would have fitted are lost
//! with it.

use rasterizeddb_compat::TestServer;
use sqlx::{AssertSqlSafe, Executor, Row};

async fn server() -> (TestServer, sqlx::PgPool) {
    let server = TestServer::start().await.expect("server should start");
    let pool = server.connect_sqlx().await.expect("should connect");
    pool.execute(sqlx::raw_sql(
        "CREATE TABLE page (row_id TEXT NOT NULL UNIQUE, entry_order INT, label TEXT)",
    ))
    .await
    .expect("create");
    (server, pool)
}

async fn row_ids(pool: &sqlx::PgPool) -> Vec<String> {
    let rows = sqlx::query("SELECT row_id FROM page ORDER BY row_id")
        .fetch_all(pool)
        .await
        .expect("select");
    rows.iter().map(|r| r.get::<String, _>("row_id")).collect()
}

/// One tuple conflicts, two do not, every value bound rather than inlined.
#[tokio::test(flavor = "multi_thread")]
async fn a_bound_multirow_upsert_skips_the_conflict_and_keeps_the_rest() {
    let (_server, pool) = server().await;

    sqlx::query("INSERT INTO page (row_id, entry_order, label) VALUES ($1, $2, $3)")
        .bind("b")
        .bind(2i32)
        .bind("first")
        .execute(&pool)
        .await
        .expect("seed");

    let result = sqlx::query(
        "INSERT INTO page (row_id, entry_order, label) \
         VALUES ($1, $2, $3), ($4, $5, $6), ($7, $8, $9) \
         ON CONFLICT (row_id) DO NOTHING",
    )
    .bind("a")
    .bind(1i32)
    .bind("new")
    .bind("b")
    .bind(2i32)
    .bind("conflicting")
    .bind("c")
    .bind(3i32)
    .bind("new")
    .execute(&pool)
    .await;

    let result = result.unwrap_or_else(|e| {
        panic!(
            "a bound multi-row upsert raised instead of skipping: {e}. \
             The batch stops on the first error, so the tuples that did not \
             conflict were lost with it"
        )
    });

    assert_eq!(row_ids(&pool).await, vec!["a", "b", "c"], "the non-conflicting rows are missing");
    assert_eq!(
        result.rows_affected(),
        2,
        "the count must be rows inserted, not tuples sent: one was skipped"
    );

    // The seeded row keeps its original value — DO NOTHING, not DO UPDATE.
    let label: String = sqlx::query("SELECT label FROM page WHERE row_id = $1")
        .bind("b")
        .fetch_one(&pool)
        .await
        .expect("select")
        .get("label");
    assert_eq!(label, "first", "DO NOTHING overwrote the existing row");
}

/// Every tuple conflicts — the idempotent re-run, which must be a no-op.
#[tokio::test(flavor = "multi_thread")]
async fn a_bound_upsert_where_every_tuple_conflicts_is_a_no_op() {
    let (_server, pool) = server().await;

    let insert = "INSERT INTO page (row_id, entry_order, label) \
                  VALUES ($1, $2, $3), ($4, $5, $6) ON CONFLICT (row_id) DO NOTHING";
    for _ in 0..2 {
        let result = sqlx::query(insert)
            .bind("x")
            .bind(1i32)
            .bind("one")
            .bind("y")
            .bind(2i32)
            .bind("two")
            .execute(&pool)
            .await
            .expect("an all-conflict re-issue is the success case for an idempotent copy");
        let _ = result;
    }
    assert_eq!(row_ids(&pool).await, vec!["x", "y"]);
}

/// A page the size PMS actually sends, bound rather than inlined.
///
/// Size is the axis every simple-protocol probe already cleared, so this is
/// here to rule out an interaction between the substitution of many parameters
/// and the tuple splitting — 20 000 placeholders is where PMS lives, not 9.
#[tokio::test(flavor = "multi_thread")]
async fn a_large_bound_page_keeps_its_conflict_clause() {
    let (_server, pool) = server().await;
    const ROWS: usize = 300;

    sqlx::query("INSERT INTO page (row_id, entry_order, label) VALUES ($1, $2, $3)")
        .bind("row-0150")
        .bind(150i32)
        .bind("seeded")
        .execute(&pool)
        .await
        .expect("seed");

    let tuples: Vec<String> = (0..ROWS)
        .map(|i| format!("(${}, ${}, ${})", i * 3 + 1, i * 3 + 2, i * 3 + 3))
        .collect();
    let sql = format!(
        "INSERT INTO page (row_id, entry_order, label) VALUES {} ON CONFLICT (row_id) DO NOTHING",
        tuples.join(", ")
    );

    let mut query = sqlx::query(AssertSqlSafe(sql));
    for i in 0..ROWS {
        query = query
            .bind(format!("row-{i:04}"))
            .bind(i as i32)
            .bind(format!("label-{i}"));
    }
    let result = query
        .execute(&pool)
        .await
        .expect("a large bound page must skip its one conflict rather than raise");

    assert_eq!(
        result.rows_affected(),
        (ROWS - 1) as u64,
        "one tuple conflicted, so one fewer row than tuples sent"
    );
    let count: i64 = pool
        .fetch_one(sqlx::raw_sql("SELECT COUNT(*) AS c FROM page"))
        .await
        .expect("count")
        .get("c");
    assert_eq!(count, ROWS as i64);
    // The seeded row is untouched.
    let label: String = sqlx::query("SELECT label FROM page WHERE row_id = $1")
        .bind("row-0150")
        .fetch_one(&pool)
        .await
        .expect("select")
        .get("label");
    assert_eq!(label, "seeded");
}

/// The same thing with every identifier quoted, as an ORM emits them.
///
/// A separate test because quoting is the class of difference this engine has
/// been bitten by before — the tuple splitter and the per-row re-parser are
/// scanners, and a scanner that does not track quoting drops or keeps the wrong
/// span. PMS's companion tables are named `"utrn_<hex>"` with columns
/// `"c_<hex>"`, so every identifier in the real statement is quoted and none of
/// the probes above had one.
#[tokio::test(flavor = "multi_thread")]
async fn a_bound_upsert_with_quoted_identifiers_keeps_its_conflict_clause() {
    let server = TestServer::start().await.expect("server should start");
    let pool = server.connect_sqlx().await.expect("should connect");
    pool.execute(sqlx::raw_sql(
        "CREATE TABLE \"utrn_ffe8aa15\" (\"row_id\" TEXT NOT NULL UNIQUE, \
         \"entry_order\" INT, \"c_ae1b87f5\" TEXT)",
    ))
    .await
    .expect("create");

    sqlx::query(
        "INSERT INTO \"utrn_ffe8aa15\" (\"row_id\", \"entry_order\", \"c_ae1b87f5\") \
         VALUES ($1, $2, $3)",
    )
    .bind("b")
    .bind(2i32)
    .bind("first")
    .execute(&pool)
    .await
    .expect("seed");

    let result = sqlx::query(
        "INSERT INTO \"utrn_ffe8aa15\" (\"row_id\", \"entry_order\", \"c_ae1b87f5\") \
         VALUES ($1, $2, $3), ($4, $5, $6), ($7, $8, $9) \
         ON CONFLICT (\"row_id\") DO NOTHING",
    )
    .bind("a")
    .bind(1i32)
    .bind("new")
    .bind("b")
    .bind(2i32)
    .bind("conflicting")
    .bind("c")
    .bind(3i32)
    .bind("new")
    .execute(&pool)
    .await
    .unwrap_or_else(|e| {
        panic!("a quoted-identifier multi-row upsert raised instead of skipping: {e}")
    });

    assert_eq!(result.rows_affected(), 2);
    let count: i64 = pool
        .fetch_one(sqlx::raw_sql("SELECT COUNT(*) AS c FROM \"utrn_ffe8aa15\""))
        .await
        .expect("count")
        .get("c");
    assert_eq!(count, 3, "the non-conflicting rows are missing");
}

/// `ON CONFLICT` with an unquoted target against a quoted column, and a
/// `RETURNING` after it — the ordering the splitter has to preserve.
#[tokio::test(flavor = "multi_thread")]
async fn a_bound_upsert_keeps_conflict_and_returning_together() {
    let (_server, pool) = server().await;

    sqlx::query("INSERT INTO page (row_id, entry_order, label) VALUES ($1, $2, $3)")
        .bind("b")
        .bind(2i32)
        .bind("first")
        .execute(&pool)
        .await
        .expect("seed");

    let returned = sqlx::query(
        "INSERT INTO page (row_id, entry_order, label) VALUES ($1, $2, $3), ($4, $5, $6) \
         ON CONFLICT (row_id) DO NOTHING RETURNING row_id",
    )
    .bind("a")
    .bind(1i32)
    .bind("new")
    .bind("b")
    .bind(2i32)
    .bind("conflicting")
    .fetch_all(&pool)
    .await
    .unwrap_or_else(|e| panic!("conflict-plus-returning raised: {e}"));

    let ids: Vec<String> = returned.iter().map(|r| r.get::<String, _>("row_id")).collect();
    assert_eq!(ids, vec!["a"], "RETURNING must report the inserted row and not the skipped one");
}

/// Two tuples in one statement carrying the same key.
///
/// Suggested by the production error naming a position — `row 1 already holds
/// it` — which is what an intra-statement conflict would look like. The engine
/// executes the tuples one at a time, so the second conflicts against a row the
/// first inserted and has not committed. `DO NOTHING` must skip it; PostgreSQL
/// does, and only raises for `DO UPDATE` affecting a row twice.
#[tokio::test(flavor = "multi_thread")]
async fn a_key_repeated_inside_one_statement_is_skipped_not_raised() {
    let (_server, pool) = server().await;

    let result = sqlx::query(
        "INSERT INTO page (row_id, entry_order, label) \
         VALUES ($1, $2, $3), ($4, $5, $6), ($7, $8, $9) \
         ON CONFLICT (row_id) DO NOTHING",
    )
    .bind("dup")
    .bind(1i32)
    .bind("first")
    .bind("dup")
    .bind(2i32)
    .bind("repeat")
    .bind("other")
    .bind(3i32)
    .bind("third")
    .execute(&pool)
    .await
    .unwrap_or_else(|e| {
        panic!(
            "a key repeated inside one statement raised instead of being skipped: {e}. \
             The batch stops on the first error, so every later tuple is lost too"
        )
    });

    assert_eq!(result.rows_affected(), 2, "one of the two duplicates should have been skipped");
    assert_eq!(row_ids(&pool).await, vec!["dup", "other"]);
    let label: String = sqlx::query("SELECT label FROM page WHERE row_id = $1")
        .bind("dup")
        .fetch_one(&pool)
        .await
        .expect("select")
        .get("label");
    assert_eq!(label, "first", "the first tuple's value must win");
}

/// A conflict against a row inserted earlier in the same transaction.
///
/// The other way an uncommitted row becomes the thing being conflicted with.
/// An ORM pages inside one transaction, so page N+1 routinely conflicts with
/// rows page N inserted and has not yet committed.
#[tokio::test(flavor = "multi_thread")]
async fn a_conflict_against_an_uncommitted_row_in_the_same_transaction_is_skipped() {
    let (_server, pool) = server().await;
    let mut tx = pool.begin().await.expect("begin");

    sqlx::query("INSERT INTO page (row_id, entry_order, label) VALUES ($1, $2, $3), ($4, $5, $6)")
        .bind("p1")
        .bind(1i32)
        .bind("one")
        .bind("p2")
        .bind(2i32)
        .bind("two")
        .execute(&mut *tx)
        .await
        .expect("first page");

    let result = sqlx::query(
        "INSERT INTO page (row_id, entry_order, label) VALUES ($1, $2, $3), ($4, $5, $6) \
         ON CONFLICT (row_id) DO NOTHING",
    )
    .bind("p2")
    .bind(2i32)
    .bind("overlap")
    .bind("p3")
    .bind(3i32)
    .bind("three")
    .execute(&mut *tx)
    .await
    .unwrap_or_else(|e| {
        panic!("an overlapping second page raised inside one transaction: {e}")
    });

    assert_eq!(result.rows_affected(), 1, "only p3 is new");
    tx.commit().await.expect("commit");
    assert_eq!(row_ids(&pool).await, vec!["p1", "p2", "p3"]);
}

/// Two connections upserting overlapping keys at the same time.
///
/// The last axis a probe on this side can reach, and the one that fits the
/// production symptom: a backfill paging into a table the application is also
/// writing. `ON CONFLICT DO NOTHING` against a row another transaction has
/// inserted but not committed is the interesting case — PostgreSQL waits for
/// that transaction and then skips the row.
///
/// This engine is documented as refusing concurrent writers with `40001`
/// instead of blocking, so a serialisation failure here is *today's* accepted
/// behaviour and this test allows it. What it does not allow is a **duplicate
/// key** error, because that is the error `DO NOTHING` exists to absorb, and a
/// caller that correctly retries on `40001` has no reason to retry on it — it
/// reads as a permanent constraint violation, and the batch loses every tuple
/// after it.
///
/// # Ignored for the same open bug as the last test in this file
///
/// Its error assertion passes; its *data* assertion does not, once the two
/// statements genuinely contend — which happens only under suite load. This
/// test passes when run alone and fails when run with the others, because
/// spawning two tasks does not by itself make them overlap. Worth knowing about
/// every test of this shape: the concurrency is incidental, and the counting is
/// what detects the failure.
#[tokio::test(flavor = "multi_thread")]
async fn a_concurrent_upsert_never_reports_a_duplicate_key() {
    let (_server, pool) = server().await;

    let mut failures: Vec<String> = Vec::new();
    for round in 0..8i32 {
        let shared = format!("shared-{round}");
        let one = {
            let pool = pool.clone();
            let shared = shared.clone();
            tokio::spawn(async move {
                sqlx::query(
                    "INSERT INTO page (row_id, entry_order, label) VALUES ($1, $2, $3), ($4, $5, $6) \
                     ON CONFLICT (row_id) DO NOTHING",
                )
                .bind(&shared)
                .bind(round)
                .bind("a")
                .bind(format!("only-a-{round}"))
                .bind(round)
                .bind("a")
                .execute(&pool)
                .await
                .map(|_| ())
                .map_err(|e| e.to_string())
            })
        };
        let two = {
            let pool = pool.clone();
            let shared = shared.clone();
            tokio::spawn(async move {
                sqlx::query(
                    "INSERT INTO page (row_id, entry_order, label) VALUES ($1, $2, $3), ($4, $5, $6) \
                     ON CONFLICT (row_id) DO NOTHING",
                )
                .bind(&shared)
                .bind(round)
                .bind("b")
                .bind(format!("only-b-{round}"))
                .bind(round)
                .bind("b")
                .execute(&pool)
                .await
                .map(|_| ())
                .map_err(|e| e.to_string())
            })
        };
        for outcome in [one.await.expect("task"), two.await.expect("task")] {
            if let Err(message) = outcome {
                failures.push(message);
            }
        }
    }

    // A serialisation failure is the engine's documented answer to concurrent
    // writers and is not what this test is hunting.
    let duplicates: Vec<&String> = failures
        .iter()
        .filter(|message| {
            let lower = message.to_lowercase();
            lower.contains("duplicate") || lower.contains("already holds")
        })
        .collect();
    assert!(
        duplicates.is_empty(),
        "ON CONFLICT DO NOTHING raised a duplicate-key error under concurrency, which is \
         the error it exists to absorb and which a caller retrying 40001 will not retry: \
         {duplicates:?}"
    );

    // And — the assertion this test was missing — that the *data* is right.
    // Checking only for a raised error is checking the symptom that shows;
    // a uniqueness check that two writers both pass before either commits
    // produces no error at all, it produces duplicate rows in a UNIQUE column.
    // A version of this test that stopped at the line above would pass while
    // the table quietly filled with them.
    assert_no_duplicate_row_ids(&pool, "page").await;
}

/// Fails if a UNIQUE column holds the same value twice.
///
/// Split out because it is the assertion that matters for every concurrent
/// upsert, and because counting is the only way to see this failure: it raises
/// nothing, logs nothing, and every individual statement reports success.
async fn assert_no_duplicate_row_ids(pool: &sqlx::PgPool, table: &str) {
    let total: i64 = pool
        .fetch_one(sqlx::raw_sql(AssertSqlSafe(format!(
            "SELECT COUNT(*) AS c FROM {table}"
        ))))
        .await
        .expect("count")
        .get("c");
    // Counted client-side: this engine has no `COUNT(DISTINCT …)`, and the
    // point is the comparison rather than where it is computed.
    let ids: Vec<String> = sqlx::query(AssertSqlSafe(format!("SELECT row_id FROM {table}")))
        .fetch_all(pool)
        .await
        .expect("select row_id")
        .iter()
        .map(|r| r.get::<String, _>("row_id"))
        .collect();
    let distinct = ids.iter().collect::<std::collections::HashSet<_>>().len() as i64;
    assert_eq!(
        total, distinct,
        "{table} holds {} rows for {distinct} distinct row_id values, so a UNIQUE column \
         has duplicates. Nothing raised: every statement reported success",
        total
    );
}

/// The uncommitted-concurrent case, forced rather than raced.
///
/// The test above spawns two upserts and hopes they overlap; in practice they
/// did not — it recorded zero failures of any kind, so it demonstrates that the
/// happy path survives concurrency and nothing more. This one holds a
/// transaction open across the second statement, so the row genuinely exists
/// and is genuinely uncommitted when `ON CONFLICT DO NOTHING` meets it. That is
/// the state a backfill hits when the application is writing the same keys.
///
/// Three outcomes are defensible: skip it (PostgreSQL, after waiting), refuse
/// with `40001` (this engine's documented answer to concurrent writers), or
/// insert it and let the commit fail. A **duplicate-key** error is the one that
/// is not, because it is exactly what the clause exists to absorb — a caller
/// that retries `40001` reads it as a permanent constraint violation and gives
/// up, losing every tuple after it in the batch.
#[tokio::test(flavor = "multi_thread")]
async fn an_upsert_meeting_an_uncommitted_row_from_another_connection_is_not_a_duplicate_key() {
    let (_server, pool) = server().await;

    let mut holder = pool.begin().await.expect("begin");
    sqlx::query("INSERT INTO page (row_id, entry_order, label) VALUES ($1, $2, $3)")
        .bind("contended")
        .bind(1i32)
        .bind("uncommitted")
        .execute(&mut *holder)
        .await
        .expect("the holding transaction's insert");

    // A second connection, while the first is still open.
    let outcome = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        sqlx::query(
            "INSERT INTO page (row_id, entry_order, label) VALUES ($1, $2, $3), ($4, $5, $6) \
             ON CONFLICT (row_id) DO NOTHING",
        )
        .bind("contended")
        .bind(2i32)
        .bind("second")
        .bind("uncontended")
        .bind(2i32)
        .bind("second")
        .execute(&pool),
    )
    .await;

    let _ = holder.rollback().await;

    match outcome {
        // Blocking until the holder finishes is PostgreSQL's behaviour and
        // would be an improvement, not a failure.
        Err(_elapsed) => {}
        Ok(Ok(_)) => {}
        Ok(Err(e)) => {
            let message = e.to_string().to_lowercase();
            assert!(
                !(message.contains("duplicate") || message.contains("already holds")),
                "ON CONFLICT DO NOTHING reported a duplicate key against an uncommitted row \
                 from another connection. That is the error the clause exists to absorb, and \
                 a caller retrying 40001 will not retry it: {e}"
            );
        }
    }
}

/// A `uuid` PRIMARY KEY rather than a `TEXT UNIQUE` column, with `$N::type`
/// casts inside the tuples.
///
/// Both axes were eliminated by hand against a deployed node; they are here so
/// the elimination outlives the session that did it. They matter because they
/// are the shape the real caller emits and nothing else in this file covered
/// them: every other probe uses `TEXT UNIQUE` and bare `$N`.
///
/// The distinction that made this worth checking is that `PRIMARY KEY` and
/// `UNIQUE` need not share an enforcement path, and a cast wraps the parameter
/// so the tuple splitter sees `($1::uuid, $2::int)` rather than `($1, $2)` —
/// one more thing a scanner can mis-span.
#[tokio::test(flavor = "multi_thread")]
async fn a_uuid_primary_key_with_casts_keeps_its_conflict_clause() {
    let server = TestServer::start().await.expect("server should start");
    let pool = server.connect_sqlx().await.expect("should connect");
    pool.execute(sqlx::raw_sql(
        "CREATE TABLE keyed (row_id UUID PRIMARY KEY, entry_order INT, label TEXT)",
    ))
    .await
    .expect("create");

    let existing = "3f2504e0-4f89-11d3-9a0c-0305e82c3301";
    let fresh = "3f2504e0-4f89-11d3-9a0c-0305e82c3302";

    sqlx::query("INSERT INTO keyed (row_id, entry_order, label) VALUES ($1::uuid, $2::int, $3)")
        .bind(existing)
        .bind(1i32)
        .bind("seeded")
        .execute(&pool)
        .await
        .expect("seed");

    let result = sqlx::query(
        "INSERT INTO keyed (row_id, entry_order, label) \
         VALUES ($1::uuid, $2::int, $3), ($4::uuid, $5::int, $6) \
         ON CONFLICT (row_id) DO NOTHING",
    )
    .bind(existing)
    .bind(1i32)
    .bind("conflicting")
    .bind(fresh)
    .bind(2i32)
    .bind("new")
    .execute(&pool)
    .await
    .unwrap_or_else(|e| {
        panic!("a uuid primary key with casts raised instead of skipping: {e}")
    });

    assert_eq!(result.rows_affected(), 1, "one tuple conflicted, one was new");

    let count: i64 = pool
        .fetch_one(sqlx::raw_sql("SELECT COUNT(*) AS c FROM keyed"))
        .await
        .expect("count")
        .get("c");
    assert_eq!(count, 2);

    // The seeded row keeps its value, and an all-conflict re-issue is a no-op.
    let again = sqlx::query(
        "INSERT INTO keyed (row_id, entry_order, label) \
         VALUES ($1::uuid, $2::int, $3), ($4::uuid, $5::int, $6) \
         ON CONFLICT (row_id) DO NOTHING",
    )
    .bind(existing)
    .bind(1i32)
    .bind("x")
    .bind(fresh)
    .bind(2i32)
    .bind("y")
    .execute(&pool)
    .await
    .expect("the idempotent re-run is the success case");
    assert_eq!(again.rows_affected(), 0);

    let label: String = sqlx::query("SELECT label FROM keyed WHERE row_id = $1::uuid")
        .bind(existing)
        .fetch_one(&pool)
        .await
        .expect("select")
        .get("label");
    assert_eq!(label, "seeded", "DO NOTHING overwrote a row it should have skipped");
}

// ---------------------------------------------------------------------------
// The production shape, decomposed
// ---------------------------------------------------------------------------
//
// A captured failure showed `ON CONFLICT (row_id) DO NOTHING` present in the
// statement tail on the wire while the engine still raised a duplicate key —
// so the clause was parsed away, not omitted. Three things separated that
// statement from every probe above: its size after `$N` substitution (20 000
// parameters, far larger than the 258 KB sent), a `::jsonb` cast among the
// columns, and contention.
//
// These take the first two one at a time rather than together. A single
// combined test that fails says "one of these three", which is where the
// investigation already is.

/// Size alone: the production row and column count, no JSON anywhere.
#[tokio::test(flavor = "multi_thread")]
async fn a_thousand_rows_of_twenty_plain_columns_keeps_its_conflict_clause() {
    let server = TestServer::start().await.expect("server should start");
    let pool = server.connect_sqlx().await.expect("should connect");
    let columns: Vec<String> = (0..19).map(|c| format!("c_{c:02} TEXT")).collect();
    pool.execute(sqlx::raw_sql(AssertSqlSafe(format!(
        "CREATE TABLE wide_plain (row_id TEXT NOT NULL UNIQUE, {})",
        columns.join(", ")
    ))))
    .await
    .expect("create");

    let names: Vec<String> = (0..19).map(|c| format!("c_{c:02}")).collect();
    let head = format!("INSERT INTO wide_plain (row_id, {}) VALUES ", names.join(", "));

    const ROWS: usize = 1000;
    const COLS: usize = 20;
    let tuples: Vec<String> = (0..ROWS)
        .map(|r| {
            let ps: Vec<String> = (0..COLS).map(|c| format!("${}", r * COLS + c + 1)).collect();
            format!("({})", ps.join(", "))
        })
        .collect();
    let sql = format!("{head}{} ON CONFLICT (row_id) DO NOTHING", tuples.join(", "));

    // Seeded so exactly one tuple conflicts, which is what makes a lost clause
    // an error rather than a silent no-op.
    sqlx::query("INSERT INTO wide_plain (row_id) VALUES ($1)")
        .bind("r-0500")
        .execute(&pool)
        .await
        .expect("seed");

    let mut query = sqlx::query(AssertSqlSafe(sql));
    for r in 0..ROWS {
        query = query.bind(format!("r-{r:04}"));
        for c in 1..COLS {
            query = query.bind(format!("v{r}-{c}"));
        }
    }
    let result = query.execute(&pool).await.unwrap_or_else(|e| {
        panic!("1000 rows x 20 plain columns ({} params) lost its clause: {e}", ROWS * COLS)
    });
    assert_eq!(result.rows_affected(), (ROWS - 1) as u64);
}

/// The JSON value class alone: small statement, awkward documents.
///
/// Braces, nested double quotes inside a single-quoted literal, an embedded
/// apostrophe (which the wire renders as `''`), a parenthesis, and the literal
/// text `ON CONFLICT` inside a value — every way a quote- and paren-toggling
/// scan can lose its place.
#[tokio::test(flavor = "multi_thread")]
async fn json_values_do_not_hide_the_conflict_clause() {
    let server = TestServer::start().await.expect("server should start");
    let pool = server.connect_sqlx().await.expect("should connect");
    pool.execute(sqlx::raw_sql(
        "CREATE TABLE docs (row_id TEXT NOT NULL UNIQUE, body JSONB, note TEXT)",
    ))
    .await
    .expect("create");

    sqlx::query("INSERT INTO docs (row_id, body, note) VALUES ($1, $2::jsonb, $3)")
        .bind("d1")
        .bind(r#"{"k":"v"}"#)
        .bind("seeded")
        .execute(&pool)
        .await
        .expect("seed");

    let awkward = r#"{"quote":"it's","paren":"a (b) c","kw":"ON CONFLICT (row_id) DO NOTHING","nested":{"a":["x","y"]}}"#;
    let result = sqlx::query(
        "INSERT INTO docs (row_id, body, note) \
         VALUES ($1, $2::jsonb, $3), ($4, $5::jsonb, $6) \
         ON CONFLICT (row_id) DO NOTHING",
    )
    .bind("d1")
    .bind(awkward)
    .bind("conflicting")
    .bind("d2")
    .bind(awkward)
    .bind("new")
    .execute(&pool)
    .await
    .unwrap_or_else(|e| panic!("a jsonb value hid the conflict clause: {e}"));

    assert_eq!(result.rows_affected(), 1, "one conflicted, one was new");
    let note: String = sqlx::query("SELECT note FROM docs WHERE row_id = $1")
        .bind("d1")
        .fetch_one(&pool)
        .await
        .expect("select")
        .get("note");
    assert_eq!(note, "seeded", "DO NOTHING overwrote the row it should have skipped");
}

/// Both together: the production shape as closely as this harness can build it.
///
/// 1000 rows x 20 columns with a `::jsonb` document in every one, so there are
/// a thousand opportunities for a quote- or paren-toggling scan to lose its
/// place before it reaches the clause at the end. The documents carry the same
/// awkward content as the isolated JSON test — an apostrophe, a parenthesis,
/// nested quotes, and the literal text of the clause itself.
#[tokio::test(flavor = "multi_thread")]
async fn a_thousand_rows_with_a_json_column_keeps_its_conflict_clause() {
    let server = TestServer::start().await.expect("server should start");
    let pool = server.connect_sqlx().await.expect("should connect");
    let columns: Vec<String> = (0..18).map(|c| format!("c_{c:02} TEXT")).collect();
    pool.execute(sqlx::raw_sql(AssertSqlSafe(format!(
        "CREATE TABLE wide_json (row_id TEXT NOT NULL UNIQUE, body JSONB, {})",
        columns.join(", ")
    ))))
    .await
    .expect("create");

    let names: Vec<String> = (0..18).map(|c| format!("c_{c:02}")).collect();
    let head = format!("INSERT INTO wide_json (row_id, body, {}) VALUES ", names.join(", "));

    const ROWS: usize = 1000;
    const COLS: usize = 20;
    let tuples: Vec<String> = (0..ROWS)
        .map(|r| {
            let base = r * COLS;
            let mut ps = vec![format!("${}", base + 1), format!("${}::jsonb", base + 2)];
            ps.extend((2..COLS).map(|c| format!("${}", base + c + 1)));
            format!("({})", ps.join(", "))
        })
        .collect();
    let sql = format!("{head}{} ON CONFLICT (row_id) DO NOTHING", tuples.join(", "));

    sqlx::query("INSERT INTO wide_json (row_id) VALUES ($1)")
        .bind("r-0500")
        .execute(&pool)
        .await
        .expect("seed");

    let doc = r#"{"quote":"it's","paren":"a (b) c","kw":"ON CONFLICT (row_id) DO NOTHING"}"#;
    let mut query = sqlx::query(AssertSqlSafe(sql));
    for r in 0..ROWS {
        query = query.bind(format!("r-{r:04}")).bind(doc);
        for c in 2..COLS {
            query = query.bind(format!("v{r}-{c}"));
        }
    }
    let result = query.execute(&pool).await.unwrap_or_else(|e| {
        panic!(
            "1000 rows x 20 columns with a jsonb column ({} params) lost its clause: {e}",
            ROWS * COLS
        )
    });
    assert_eq!(result.rows_affected(), (ROWS - 1) as u64);
}

/// Values that look like placeholders, dollar-quoting, or comments.
///
/// The gap every test above leaves: what PMS logs is the statement *it sent*,
/// but the parser sees the text after `inline_params` has expanded the
/// placeholders. Those differ if a bound value is itself re-read as syntax.
/// User data in a JSON column is exactly where `$1`, `$tag$`, `--` and `/*`
/// arrive without anyone intending them.
///
/// If substitution re-scanned its own output, a value containing `$19999` would
/// be replaced again and the tail of the statement — which is where
/// `ON CONFLICT` lives — would be rewritten out from under the parser, with the
/// wire text still looking correct in any log taken on the sending side.
#[tokio::test(flavor = "multi_thread")]
async fn values_that_look_like_syntax_do_not_rewrite_the_statement() {
    let server = TestServer::start().await.expect("server should start");
    let pool = server.connect_sqlx().await.expect("should connect");
    pool.execute(sqlx::raw_sql(
        "CREATE TABLE tricky (row_id TEXT NOT NULL UNIQUE, body JSONB, note TEXT)",
    ))
    .await
    .expect("create");

    sqlx::query("INSERT INTO tricky (row_id, body, note) VALUES ($1, $2::jsonb, $3)")
        .bind("t1")
        .bind(r#"{"k":"v"}"#)
        .bind("seeded")
        .execute(&pool)
        .await
        .expect("seed");

    // Every shape the substituter and the scanners give meaning to.
    let hostile = r#"{"a":"$1","b":"$19999","c":"$tag$ x $tag$","d":"-- comment","e":"/* block */","f":"'","g":"\"","h":";"}"#;

    let result = sqlx::query(
        "INSERT INTO tricky (row_id, body, note) \
         VALUES ($1, $2::jsonb, $3), ($4, $5::jsonb, $6) \
         ON CONFLICT (row_id) DO NOTHING",
    )
    .bind("t1")
    .bind(hostile)
    .bind("conflicting")
    .bind("t2")
    .bind(hostile)
    .bind("new")
    .execute(&pool)
    .await
    .unwrap_or_else(|e| {
        panic!("a value that looks like syntax cost the statement its clause: {e}")
    });

    assert_eq!(result.rows_affected(), 1, "one conflicted, one was new");

    // And the value round-trips unchanged — a substituter that re-read its own
    // output would corrupt the stored document as well as the statement.
    let note: String = sqlx::query("SELECT note FROM tricky WHERE row_id = $1")
        .bind("t2")
        .fetch_one(&pool)
        .await
        .expect("select")
        .get("note");
    assert_eq!(note, "new");
}

/// Several clients sending the *same* page at once — the idempotent-copy shape.
///
/// This is the failure another session reproduced on a file-backed instance: 4
/// concurrent identical 500-row upserts produced 1959 rows for 500 distinct
/// keys, with no error raised anywhere. Two writers pass the uniqueness check
/// before either commits and both write; without `ON CONFLICT` the loser's
/// violation aborts its statement and its rows go away, but with the clause the
/// violation is swallowed per row and the statement commits what it already
/// wrote. So the clause turns an aborted duplicate into a stored one.
///
/// It is the assertion, not the concurrency, that makes this test work. An
/// earlier version of the test above checked only that no duplicate-key *error*
/// was raised — and this failure raises nothing, logs nothing, and reports
/// success on every statement. Only counting the rows can see it.
///
/// # Ignored because it currently fails
///
/// It reproduces here in a tenth of a second and **in memory** — 768 rows for
/// 200 distinct keys on this harness — where the original reproduction needed a
/// file-backed scratch instance. So the race is not specific to the storage
/// path, and this is a cheap regression test for the fix rather than only a
/// demonstration of the bug.
///
/// Ignored rather than left red so a shared suite stays green while the fix is
/// decided. **Remove the `#[ignore]` with the fix** — a test that documents a
/// known-bad behaviour is worth nothing if nobody notices it starting to pass.
#[tokio::test(flavor = "multi_thread")]
async fn identical_pages_sent_at_once_do_not_duplicate_rows() {
    let (_server, pool) = server().await;
    const WRITERS: usize = 4;
    const ROWS: usize = 200;

    let placeholders: Vec<String> = (0..ROWS)
        .map(|r| format!("(${}, ${}, ${})", r * 3 + 1, r * 3 + 2, r * 3 + 3))
        .collect();
    let sql = format!(
        "INSERT INTO page (row_id, entry_order, label) VALUES {} ON CONFLICT (row_id) DO NOTHING",
        placeholders.join(", ")
    );

    let mut writers = Vec::new();
    for writer in 0..WRITERS {
        let pool = pool.clone();
        let sql = sql.clone();
        writers.push(tokio::spawn(async move {
            let mut query = sqlx::query(AssertSqlSafe(sql));
            for r in 0..ROWS {
                query = query
                    .bind(format!("k-{r:04}"))
                    .bind(r as i32)
                    .bind(format!("writer-{writer}"));
            }
            query.execute(&pool).await.map(|_| ()).map_err(|e| e.to_string())
        }));
    }
    for writer in writers {
        // A failure is allowed — a serialisation refusal is this engine's
        // documented answer to concurrent writers. Duplicated rows are not.
        let _ = writer.await.expect("task");
    }

    assert_no_duplicate_row_ids(&pool, "page").await;
}
