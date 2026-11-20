//! A simple-query `SELECT` learns a parse for its template, its `WHERE`
//! values taken out (`pgwire::text_template`), the way a prepared statement
//! does, and then runs from it. The cache may only ever be faster: every
//! answer here is compared with the plain text path's (the cache switched
//! off), for values the cache was and was not learned with.
//!
//! What must keep the text path is pinned through the engine's count of
//! template runs: a value a rewrite reads (`LIKE`, a `NULL`, a negative
//! number, a string holding a quote), and a schema change, after which the
//! template is learned again.

use rasterizeddb_compat::TestServer;
use rasterizeddb_core::pgwire::{TEXT_RUNS, set_text_shapes};
use sqlx::{AssertSqlSafe, Connection, Executor, PgConnection, Row};
use std::sync::atomic::Ordering;

/// The switch and the count are process-wide: one test at a time.
static ONE_AT_A_TIME: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn runs() -> u64 {
    TEXT_RUNS.load(Ordering::Relaxed)
}

fn render(row: &sqlx::postgres::PgRow) -> String {
    (0..row.len())
        .map(|i| {
            if let Ok(v) = row.try_get::<Option<i64>, _>(i) {
                return format!("{v:?}");
            }
            if let Ok(v) = row.try_get::<Option<i32>, _>(i) {
                return format!("{v:?}");
            }
            if let Ok(v) = row.try_get::<Option<f64>, _>(i) {
                return format!("{v:?}");
            }
            if let Ok(v) = row.try_get::<Option<String>, _>(i) {
                return format!("{v:?}");
            }
            "?".to_string()
        })
        .collect::<Vec<_>>()
        .join("|")
}

/// The statement's rows as a simple query, or its error.
async fn simple(conn: &mut PgConnection, sql: &str) -> Result<Vec<String>, String> {
    sqlx::raw_sql(AssertSqlSafe(sql.to_string()))
        .fetch_all(&mut *conn)
        .await
        .map(|rows| rows.iter().map(render).collect())
        .map_err(|e| e.to_string())
}

async fn setup() -> (TestServer, PgConnection) {
    let server = TestServer::start().await.expect("server should start");
    let mut conn = PgConnection::connect(&server.url()).await.expect("connect");
    conn.execute("CREATE TABLE items (k BIGINT PRIMARY KEY, s TEXT NOT NULL, f DOUBLE PRECISION NOT NULL, n INT)")
        .await
        .expect("create");
    let values: Vec<String> = (1..=200)
        .map(|k| {
            let n = if k % 7 == 0 { "NULL".to_string() } else { (k % 13).to_string() };
            format!("({k}, 'name {:03}', {}, {n})", k % 50, k as f64 / 4.0)
        })
        .collect();
    conn.execute(AssertSqlSafe(format!("INSERT INTO items (k, s, f, n) VALUES {}", values.join(", "))))
        .await
        .expect("insert");
    (server, conn)
}

/// Each statement, for each value set: the text path's answer first, then
/// three runs with the cache on (watched, verified, then from the parse);
/// every answer must equal the text path's.
async fn same_both_ways(conn: &mut PgConnection, cases: &[(&str, Vec<Vec<&str>>)]) {
    for (shape, value_sets) in cases {
        let statement = |values: &[&str]| {
            let mut sql = shape.to_string();
            for (i, v) in values.iter().enumerate().rev() {
                sql = sql.replace(&format!("${}", i + 1), v);
            }
            sql
        };
        set_text_shapes(false);
        let mut expected = Vec::new();
        for values in value_sets {
            expected.push(simple(conn, &statement(values)).await);
        }
        set_text_shapes(true);
        for _round in 0..3 {
            for (values, want) in value_sets.iter().zip(&expected) {
                let sql = statement(values);
                assert_eq!(&simple(conn, &sql).await, want, "{sql}");
            }
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn learned_answers_match_the_text_path() {
    let _one = ONE_AT_A_TIME.lock().await;
    let (_server, mut conn) = setup().await;
    let before = runs();
    same_both_ways(
        &mut conn,
        &[
            ("SELECT k, s, f, n FROM items WHERE k = $1", vec![vec!["5"], vec!["77"], vec!["200"], vec!["999"]]),
            ("SELECT k, s FROM items WHERE k >= $1 AND k < $2 ORDER BY k", vec![vec!["10", "20"], vec!["190", "300"], vec!["5", "5"]]),
            ("SELECT k FROM items WHERE s = $1 ORDER BY k", vec![vec!["'name 007'"], vec!["'name 049'"], vec!["'none'"]]),
            ("SELECT COUNT(*), SUM(n) FROM items WHERE n > $1", vec![vec!["3"], vec!["11"], vec!["40"]]),
            ("SELECT k, f FROM items WHERE f > $1 AND k <= $2 ORDER BY k LIMIT 5", vec![vec!["10.5", "100"], vec!["0.25", "3"]]),
            ("SELECT n, COUNT(*) FROM items WHERE k < $1 GROUP BY n ORDER BY n", vec![vec!["50"], vec!["120"]]),
        ],
    )
    .await;
    assert!(runs() > before, "no statement ran from a learned parse");
}

/// A value a rewrite reads by what it holds keeps the text path, as it does
/// for a prepared statement; and the answers stay the text path's.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn what_a_rewrite_reads_keeps_the_text_path() {
    let _one = ONE_AT_A_TIME.lock().await;
    let (_server, mut conn) = setup().await;
    let before = runs();
    same_both_ways(
        &mut conn,
        &[
            ("SELECT k FROM items WHERE s LIKE $1 ORDER BY k", vec![vec!["'name 00%'"], vec!["'name 007'"], vec!["'%9'"]]),
            ("SELECT k FROM items WHERE k = $1", vec![vec!["NULL"], vec!["-5"]]),
            ("SELECT k FROM items WHERE s = $1", vec![vec!["'it''s'"], vec!["'o''k'"]]),
            ("SELECT k FROM items WHERE k IN ($1, $2) ORDER BY k", vec![vec!["1", "2"], vec!["3", "4"]]),
            ("SELECT k FROM items WHERE k BETWEEN $1 AND $2 ORDER BY k", vec![vec!["1", "4"], vec!["10", "12"]]),
            ("SELECT k FROM items WHERE k = $1 + 1", vec![vec!["4"], vec!["9"]]),
        ],
    )
    .await;
    assert_eq!(runs(), before, "a statement a rewrite reads ran from a learned parse");
}

/// A schema change after a template is learned: the next run must see the
/// table as it is now, its new column included, not the learned shape.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_schema_change_is_learned_again() {
    let _one = ONE_AT_A_TIME.lock().await;
    let (_server, mut conn) = setup().await;
    set_text_shapes(true);
    for k in [1, 2, 3, 4] {
        simple(&mut conn, &format!("SELECT * FROM items WHERE k = {k}")).await.expect("learn");
    }
    let learned = runs();
    simple(&mut conn, "SELECT * FROM items WHERE k = 5").await.expect("run");
    assert!(runs() > learned, "the template was not learned");

    conn.execute("ALTER TABLE items ADD COLUMN extra INT").await.expect("alter");
    conn.execute("UPDATE items SET extra = k * 2").await.expect("fill");
    let rows = simple(&mut conn, "SELECT * FROM items WHERE k = 6").await.expect("after");
    assert_eq!(rows.len(), 1);
    assert!(rows[0].ends_with("|Some(12)"), "the new column is missing: {rows:?}");

    // Dropped and made again with other types under the same name.
    conn.execute("DROP TABLE items").await.expect("drop");
    conn.execute("CREATE TABLE items (k TEXT PRIMARY KEY, v BIGINT)").await.expect("recreate");
    conn.execute("INSERT INTO items VALUES ('7', 70), ('8', 80)").await.expect("refill");
    for k in ["'7'", "'8'", "'7'"] {
        let rows = simple(&mut conn, &format!("SELECT * FROM items WHERE k = {k}")).await.expect("new table");
        assert_eq!(rows.len(), 1, "{k}: {rows:?}");
    }
}

/// A learned parse reads what the statement's transaction sees: its own
/// uncommitted rows, and not another session's.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_learned_parse_reads_through_the_transaction() {
    let _one = ONE_AT_A_TIME.lock().await;
    let (server, mut conn) = setup().await;
    set_text_shapes(true);
    for k in [1, 2, 3] {
        simple(&mut conn, &format!("SELECT k, s FROM items WHERE k = {k}")).await.expect("learn");
    }
    let mut other = PgConnection::connect(&server.url()).await.expect("connect");
    conn.execute("BEGIN").await.expect("begin");
    conn.execute("INSERT INTO items (k, s, f) VALUES (5000, 'mine', 1.0)").await.expect("insert");
    let before = runs();
    assert_eq!(simple(&mut conn, "SELECT k, s FROM items WHERE k = 5000").await.unwrap(), vec!["Some(5000)|Some(\"mine\")"]);
    assert!(runs() > before, "the learned parse did not run in the block");
    assert_eq!(simple(&mut other, "SELECT k, s FROM items WHERE k = 5000").await.unwrap(), Vec::<String>::new());
    conn.execute("ROLLBACK").await.expect("rollback");
    assert_eq!(simple(&mut conn, "SELECT k, s FROM items WHERE k = 5000").await.unwrap(), Vec::<String>::new());
}

/// A temporary table made after a template was learned on the table of the
/// same name hides it, as in PostgreSQL: the next run reads the temporary
/// one, and the real one again once it is dropped.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_temporary_table_hides_a_learned_one() {
    let _one = ONE_AT_A_TIME.lock().await;
    let (_server, mut conn) = setup().await;
    set_text_shapes(true);
    for k in [1, 2, 3, 4] {
        simple(&mut conn, &format!("SELECT k, s FROM items WHERE k = {k}")).await.expect("learn");
    }
    conn.execute("CREATE TEMP TABLE items (k BIGINT, s TEXT)").await.expect("temp");
    conn.execute("INSERT INTO items VALUES (5, 'temporary')").await.expect("temp row");
    assert_eq!(
        simple(&mut conn, "SELECT k, s FROM items WHERE k = 5").await.unwrap(),
        vec!["Some(5)|Some(\"temporary\")"]
    );
    assert_eq!(simple(&mut conn, "SELECT k, s FROM items WHERE k = 6").await.unwrap(), Vec::<String>::new());
    conn.execute("DROP TABLE items").await.expect("drop temp");
    assert_eq!(
        simple(&mut conn, "SELECT k, s FROM items WHERE k = 6").await.unwrap(),
        vec!["Some(6)|Some(\"name 006\")"]
    );
}

/// The same for a prepared statement: a parse it learned before the
/// temporary table was made must not answer for it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_temporary_table_hides_a_table_a_prepared_statement_learned() {
    let _one = ONE_AT_A_TIME.lock().await;
    let (_server, mut conn) = setup().await;
    let read = |k: i64| sqlx::query("SELECT k, s FROM items WHERE k = $1").bind(k);
    for k in [1, 2, 3, 4] {
        read(k).fetch_all(&mut conn).await.expect("learn");
    }
    conn.execute("CREATE TEMP TABLE items (k BIGINT, s TEXT)").await.expect("temp");
    conn.execute("INSERT INTO items VALUES (5, 'temporary')").await.expect("temp row");
    let rows: Vec<String> = read(5).fetch_all(&mut conn).await.expect("temp read").iter().map(render).collect();
    assert_eq!(rows, vec!["Some(5)|Some(\"temporary\")"]);
    conn.execute("DROP TABLE items").await.expect("drop temp");
    let rows: Vec<String> = read(6).fetch_all(&mut conn).await.expect("real read").iter().map(render).collect();
    assert_eq!(rows, vec!["Some(6)|Some(\"name 006\")"]);
}
