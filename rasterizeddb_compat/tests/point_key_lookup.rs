//! A learned `SELECT` whose `WHERE` pins every column of a unique index by
//! equality to a value finds its candidate rows by the bound key
//! (`prepared::IndexPlan`, `row::point_key`) instead of reading its rendered
//! `WHERE` for them. The key may only ever be faster: every answer here is
//! compared with the plain text path's (the template cache switched off),
//! through changes to the rows between rounds, and for shapes that must not
//! take a key at all (an `OR`, a range, `<>`), where a key would lose rows.
//!
//! That the key was used is read from `point_key::HITS`.

use rasterizeddb_compat::TestServer;
use rasterizeddb_core::core::row::point_key::HITS;
use rasterizeddb_core::pgwire::set_text_shapes;
use sqlx::{AssertSqlSafe, Connection, Executor, PgConnection, Row};
use std::sync::atomic::Ordering;

/// The switch and the count are process-wide: one test at a time.
static ONE_AT_A_TIME: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn hits() -> u64 {
    HITS.load(Ordering::Relaxed)
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
    conn.execute("CREATE TABLE items (k BIGINT PRIMARY KEY, s TEXT NOT NULL, n BIGINT)")
        .await
        .expect("create");
    let values: Vec<String> = (1..=200)
        .map(|k| {
            let n = if k % 7 == 0 { "NULL".to_string() } else { (k % 13).to_string() };
            format!("({k}, 'name {:03}', {n})", k % 50)
        })
        .collect();
    conn.execute(AssertSqlSafe(format!("INSERT INTO items (k, s, n) VALUES {}", values.join(", "))))
        .await
        .expect("insert");
    // A composite unique key over a text and an integer column.
    conn.execute("ALTER TABLE items ADD CONSTRAINT items_s_k UNIQUE (s, k)").await.expect("unique");
    (server, conn)
}

fn statement(shape: &str, values: &[&str]) -> String {
    let mut sql = shape.to_string();
    for (i, v) in values.iter().enumerate().rev() {
        sql = sql.replace(&format!("${}", i + 1), v);
    }
    sql
}

/// For each statement and value set: the text path's answer, then three
/// runs with the cache on (watched, verified, then learned); every answer
/// must be the text path's.
async fn same_both_ways(conn: &mut PgConnection, cases: &[(&str, Vec<Vec<&str>>)]) {
    for (shape, value_sets) in cases {
        set_text_shapes(false);
        let mut expected = Vec::new();
        for values in value_sets {
            expected.push(simple(conn, &statement(shape, values)).await);
        }
        set_text_shapes(true);
        for _round in 0..3 {
            for (values, want) in value_sets.iter().zip(&expected) {
                let sql = statement(shape, values);
                assert_eq!(&simple(conn, &sql).await, want, "{sql}");
            }
        }
    }
}

const BY_KEY: &[(&str, &[&[&str]])] = &[
    ("SELECT k, s, n FROM items WHERE k = $1", &[&["5"], &["77"], &["200"], &["999"]]),
    // A conjunct beside the key still filters the row the key finds.
    ("SELECT k FROM items WHERE k = $1 AND n > $2", &[&["5", "3"], &["77", "100"], &["12", "0"], &["999", "1"]]),
    // Both columns of the composite key, in the other order.
    ("SELECT k, n FROM items WHERE k = $1 AND s = $2", &[&["7", "'name 007'"], &["57", "'name 007'"], &["8", "'name 007'"]]),
];

fn cases(list: &'static [(&'static str, &'static [&'static [&'static str]])]) -> Vec<(&'static str, Vec<Vec<&'static str>>)> {
    list.iter().map(|(s, v)| (*s, v.iter().map(|x| x.to_vec()).collect())).collect()
}

/// A learned point read on a unique key answers as the text path, by the
/// key, while the rows it finds change between rounds: a row updated, one
/// deleted, one inserted where the key found nothing before.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_learned_point_read_finds_its_row_by_the_key() {
    let _one = ONE_AT_A_TIME.lock().await;
    let (_server, mut conn) = setup().await;
    let before = hits();
    same_both_ways(&mut conn, &cases(BY_KEY)).await;
    assert!(hits() > before, "no learned point read found its rows by the key");

    for change in [
        "UPDATE items SET n = 1000, s = 'moved' WHERE k = 77",
        "DELETE FROM items WHERE k = 5",
        "INSERT INTO items (k, s, n) VALUES (999, 'late', 4)",
        "UPDATE items SET k = 998 WHERE k = 200",
    ] {
        conn.execute(change).await.expect(change);
        same_both_ways(&mut conn, &cases(BY_KEY)).await;
    }
}

/// Shapes whose rows a key would not all find keep reading their `WHERE`:
/// the key column under an `OR`, a range or `<>`. Their answers are the
/// text path's however the cache learns them.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn what_a_key_would_not_find_reads_its_where() {
    let _one = ONE_AT_A_TIME.lock().await;
    let (_server, mut conn) = setup().await;
    let before = hits();
    same_both_ways(
        &mut conn,
        &[
            ("SELECT k FROM items WHERE k = $1 OR n = $2 ORDER BY k", vec![vec!["5", "3"], vec!["77", "11"], vec!["999", "0"]]),
            ("SELECT k FROM items WHERE k > $1 ORDER BY k", vec![vec!["195"], vec!["198"]]),
            ("SELECT COUNT(*) FROM items WHERE k <> $1", vec![vec!["5"], vec!["77"]]),
            ("SELECT k FROM items WHERE NOT (k = $1) AND n = $2 ORDER BY k", vec![vec!["5", "5"], vec!["18", "5"]]),
            // Part of the composite key only: not a point.
            ("SELECT k FROM items WHERE s = $1 ORDER BY k", vec![vec!["'name 007'"], vec!["'name 012'"]]),
        ],
    )
    .await;
    assert_eq!(hits(), before, "a shape a key does not answer took one");
}

/// A prepared statement (a named one, values bound) learns the same way.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_prepared_point_read_finds_its_row_by_the_key() {
    let _one = ONE_AT_A_TIME.lock().await;
    let (_server, mut conn) = setup().await;
    let keys = [5i64, 77, 200, 999, 13, 14];
    // The text path's answers, the template cache off: a learned simple
    // query is no oracle for a learned prepared one.
    set_text_shapes(false);
    let mut want = Vec::new();
    for k in keys {
        want.push(simple(&mut conn, &format!("SELECT k, s, n FROM items WHERE k = {k}")).await.unwrap());
    }
    set_text_shapes(true);
    let before = hits();
    for _round in 0..3 {
        for (k, want) in keys.iter().zip(&want) {
            let rows = sqlx::query("SELECT k, s, n FROM items WHERE k = $1").bind(k).fetch_all(&mut conn).await.unwrap();
            assert_eq!(&rows.iter().map(render).collect::<Vec<_>>(), want, "k = {k}");
        }
    }
    assert!(hits() > before, "no prepared point read found its rows by the key");
}

/// A schema change forgets what was learned; the next learned plan is the
/// new table's. Dropping the composite unique constraint and making a
/// different one keeps every answer the text path's.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_key_follows_the_table_through_constraint_changes() {
    let _one = ONE_AT_A_TIME.lock().await;
    let (_server, mut conn) = setup().await;
    let shapes = cases(BY_KEY);
    same_both_ways(&mut conn, &shapes).await;
    conn.execute("ALTER TABLE items DROP CONSTRAINT items_s_k").await.expect("drop");
    same_both_ways(&mut conn, &shapes).await;
    conn.execute("ALTER TABLE items ADD CONSTRAINT items_n_k UNIQUE (n, k)").await.expect("re-add");
    same_both_ways(&mut conn, &shapes).await;
    same_both_ways(
        &mut conn,
        &[("SELECT s FROM items WHERE n = $1 AND k = $2", vec![vec!["5", "5"], vec!["11", "11"], vec!["11", "12"]])],
    )
    .await;
}

/// One statement repeated with the same key, as a benchmark repeats it,
/// learns exactly (`exact`) and finds its row by the key checked for that
/// key alone; repeated with an `OR`, its key is refused and every answer
/// keeps the rows the `OR` adds. Rows change between rounds.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_repeated_statement_finds_its_row_by_the_key_checked_for_it() {
    let _one = ONE_AT_A_TIME.lock().await;
    let (_server, mut conn) = setup().await;
    let fixed = [
        "SELECT k, s, n FROM items WHERE k = 77",
        "SELECT k, n FROM items WHERE k = 57 AND s = 'name 007'",
        "SELECT k FROM items WHERE k = 13 OR n = 1 ORDER BY k",
    ];
    let changes = ["SELECT 1", "UPDATE items SET n = 1 WHERE k = 77", "DELETE FROM items WHERE k = 57", "INSERT INTO items (k, s, n) VALUES (57, 'name 007', 2)"];
    let before = hits();
    for change in changes {
        conn.execute(change).await.expect(change);
        set_text_shapes(false);
        let mut want = Vec::new();
        for sql in fixed {
            want.push(simple(&mut conn, sql).await);
        }
        set_text_shapes(true);
        for _ in 0..4 {
            for (sql, want) in fixed.iter().zip(&want) {
                assert_eq!(&simple(&mut conn, sql).await, want, "{sql}");
            }
        }
    }
    assert!(hits() > before, "no repeated statement found its row by the key");
}
