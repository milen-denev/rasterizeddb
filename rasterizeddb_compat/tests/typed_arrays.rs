//! Array columns of a declared element type, against PostgreSQL 17's answers.
//!
//! Every array column was untyped text underneath: `'{1,x}'` was stored in an
//! `INT[]` column, `'{01,+2}'` was kept as written, and `ORDER BY` and `max`
//! compared the stored bytes, so `{10}` sorted before `{9}` and, worse, the
//! element count came first (`{z}` before `{a,b}`). Each expectation here is
//! what PostgreSQL 17 answers for the same statements.

use rasterizeddb_compat::TestServer;
use sqlx::{AssertSqlSafe, Row};

async fn server() -> (TestServer, sqlx::PgPool) {
    let server = TestServer::start().await.expect("server should start");
    let pool = server.connect_sqlx().await.expect("should connect");
    (server, pool)
}

async fn run(pool: &sqlx::PgPool, sql: &str) -> Result<(), (String, String)> {
    sqlx::query(AssertSqlSafe(sql)).execute(pool).await.map(|_| ()).map_err(|e| {
        let code = e.as_database_error().and_then(|d| d.code()).map(|c| c.to_string()).unwrap_or_default();
        (code, e.to_string())
    })
}

async fn ok(pool: &sqlx::PgPool, sql: &str) {
    if let Err((code, e)) = run(pool, sql).await {
        panic!("`{sql}` must succeed, failed {code}: {e}");
    }
}

async fn refused(pool: &sqlx::PgPool, sql: &str, code: &str, words: &str) {
    match run(pool, sql).await {
        Ok(()) => panic!("`{sql}` was accepted; PostgreSQL refuses it with {code}"),
        Err((got, e)) => {
            assert_eq!(got, code, "`{sql}`: {e}");
            assert!(e.contains(words), "`{sql}`: {e}");
        }
    }
}

/// Each row's columns as text, `NULL` for a NULL, over the simple protocol.
async fn text_rows(pool: &sqlx::PgPool, sql: &str) -> Vec<Vec<String>> {
    let rows = sqlx::raw_sql(AssertSqlSafe(sql.to_string()))
        .fetch_all(pool)
        .await
        .unwrap_or_else(|e| panic!("`{sql}` should succeed: {e}"));
    rows.iter()
        .map(|row| {
            (0..row.len())
                .map(|i| {
                    let v: Option<String> = row.try_get_unchecked(i).expect("text");
                    v.unwrap_or_else(|| "NULL".to_string())
                })
                .collect()
        })
        .collect()
}

async fn column(pool: &sqlx::PgPool, sql: &str) -> Vec<String> {
    text_rows(pool, sql).await.into_iter().map(|r| r.join("|")).collect()
}

async fn table(pool: &sqlx::PgPool) {
    ok(
        pool,
        "CREATE TABLE ta (id INT, i INT[], b BOOLEAN[], n NUMERIC[], f FLOAT8[], d DATE[], t TEXT[], \
         ts TIMESTAMP[])",
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn an_element_the_type_refuses_is_refused() {
    let (_s, pool) = server().await;
    table(&pool).await;
    refused(&pool, "INSERT INTO ta (id, i) VALUES (1, '{1,x}')", "22P02", "invalid input syntax for type integer: \"x\"").await;
    refused(&pool, "INSERT INTO ta (id, b) VALUES (2, '{maybe}')", "22P02", "invalid input syntax for type boolean: \"maybe\"").await;
    refused(&pool, "INSERT INTO ta (id, d) VALUES (3, '{2020-13-01}')", "22008", "date/time field value out of range").await;
    refused(&pool, "INSERT INTO ta (id, i) VALUES (4, '{99999999999}')", "22003", "out of range for type integer").await;
    assert_eq!(column(&pool, "SELECT count(*) FROM ta").await, ["0"]);

    ok(&pool, "INSERT INTO ta (id, i) VALUES (5, '{5}')").await;
    refused(&pool, "UPDATE ta SET i = '{q}' WHERE id = 5", "22P02", "invalid input syntax for type integer: \"q\"").await;
    assert_eq!(column(&pool, "SELECT i FROM ta WHERE id = 5").await, ["{5}"]);
}

#[tokio::test(flavor = "multi_thread")]
async fn elements_are_kept_as_the_type_writes_them() {
    let (_s, pool) = server().await;
    table(&pool).await;
    ok(
        &pool,
        "INSERT INTO ta VALUES (4, '{01,+2,NULL}', '{yes,off,NULL}', '{1.50,01.0,-0}', '{1.5,1e20,-0}', \
         '{2020-01-02}', '{b,\"a b\",NULL}', '{\"2020-01-02 03:04:05\"}')",
    )
    .await;
    assert_eq!(
        text_rows(&pool, "SELECT i, b, n, f, d, t, ts FROM ta WHERE id = 4").await,
        [["{1,2,NULL}", "{t,f,NULL}", "{1.50,1.0,0}", "{1.5,1e+20,-0}", "{2020-01-02}", "{b,\"a b\",NULL}", "{\"2020-01-02 03:04:05\"}"]]
    );
    ok(&pool, "UPDATE ta SET i = '{07, 8 }' WHERE id = 4").await;
    assert_eq!(column(&pool, "SELECT i FROM ta WHERE id = 4").await, ["{7,8}"]);
    // Equal values are equal once kept alike.
    ok(&pool, "INSERT INTO ta (id, i) VALUES (5, '{7,08}')").await;
    assert_eq!(column(&pool, "SELECT count(DISTINCT i) FROM ta WHERE id IN (4, 5)").await, ["1"]);
}

#[tokio::test(flavor = "multi_thread")]
async fn arrays_order_element_by_element_as_their_type() {
    let (_s, pool) = server().await;
    table(&pool).await;
    ok(
        &pool,
        "INSERT INTO ta (id, i) VALUES (4, '{1,2,NULL}'), (5, '{10}'), (6, '{9}'), (7, '{9,1}'), (8, '{}'), \
         (9, '{NULL}'), (10, '{-3}')",
    )
    .await;
    ok(&pool, "INSERT INTO ta (id, t) VALUES (11, '{z}'), (12, '{a,b}'), (13, '{10}'), (14, '{9}')").await;
    assert_eq!(
        column(&pool, "SELECT id FROM ta WHERE i IS NOT NULL ORDER BY i, id").await,
        ["8", "10", "4", "6", "7", "5", "9"]
    );
    assert_eq!(column(&pool, "SELECT id FROM ta WHERE t IS NOT NULL ORDER BY t, id").await, ["13", "14", "12", "11"]);
    assert_eq!(text_rows(&pool, "SELECT max(i), min(i) FROM ta").await, [["{NULL}", "{}"]]);
    assert_eq!(text_rows(&pool, "SELECT max(t), min(t) FROM ta").await, [["{z}", "{10}"]]);
    assert_eq!(column(&pool, "SELECT id FROM ta WHERE i IS NOT NULL ORDER BY i DESC, id LIMIT 2").await, ["9", "5"]);
}

#[tokio::test(flavor = "multi_thread")]
async fn array_agg_orders_as_its_element_type() {
    let (_s, pool) = server().await;
    ok(&pool, "CREATE TABLE g (k INT, v INT)").await;
    ok(&pool, "INSERT INTO g VALUES (1, 10), (2, 9), (3, 100)").await;
    assert_eq!(
        column(&pool, "SELECT x FROM (SELECT array_agg(v) AS x FROM g GROUP BY k) s ORDER BY x").await,
        ["{9}", "{10}", "{100}"]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_typed_array_reads_back_in_binary_as_text_array() {
    let (_s, pool) = server().await;
    table(&pool).await;
    ok(&pool, "INSERT INTO ta (id, i, b) VALUES (1, '{01,2}', '{yes,NULL}')").await;
    // sqlx asks for binary results; the column is described as `text[]`.
    let row = sqlx::query("SELECT i, b FROM ta WHERE id = 1").fetch_one(&pool).await.expect("select");
    let i: Vec<String> = row.try_get(0).expect("int[] as text[] in binary");
    let b: Vec<Option<String>> = row.try_get(1).expect("boolean[] as text[] in binary");
    assert_eq!(i, ["1", "2"]);
    assert_eq!(b, [Some("t".to_string()), None]);
}

#[tokio::test(flavor = "multi_thread")]
async fn the_element_type_survives_a_restart() {
    let root = std::env::temp_dir().join(format!("typed_arrays_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    let root_text = root.to_string_lossy().into_owned();
    {
        let server = TestServer::start_file_backed(&root_text).await.expect("server");
        let pool = server.connect_sqlx().await.expect("should connect");
        table(&pool).await;
        ok(&pool, "INSERT INTO ta (id, i) VALUES (1, '{2}')").await;
        pool.close().await;
    }
    let server = TestServer::start_file_backed(&root_text).await.expect("restart");
    let pool = server.connect_sqlx().await.expect("should connect");
    refused(&pool, "INSERT INTO ta (id, i) VALUES (2, '{y}')", "22P02", "invalid input syntax for type integer").await;
    ok(&pool, "INSERT INTO ta (id, i) VALUES (3, '{+10}')").await;
    assert_eq!(column(&pool, "SELECT i FROM ta ORDER BY i").await, ["{2}", "{10}"]);
    pool.close().await;
    drop(server);
    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test(flavor = "multi_thread")]
async fn an_added_typed_column_is_typed_too() {
    let (_s, pool) = server().await;
    ok(&pool, "CREATE TABLE ad (id INT)").await;
    ok(&pool, "ALTER TABLE ad ADD COLUMN xs BIGINT[]").await;
    refused(&pool, "INSERT INTO ad VALUES (1, '{1.5}')", "22P02", "invalid input syntax for type bigint").await;
    ok(&pool, "INSERT INTO ad VALUES (2, '{-007}')").await;
    assert_eq!(column(&pool, "SELECT xs FROM ad").await, ["{-7}"]);
}

#[tokio::test(flavor = "multi_thread")]
async fn the_element_type_follows_the_column_through_ddl() {
    let (_s, pool) = server().await;
    ok(&pool, "CREATE TABLE lf (id INT, a INT[], b INT)").await;
    // A rename keeps it.
    ok(&pool, "ALTER TABLE lf RENAME COLUMN a TO aa").await;
    refused(&pool, "INSERT INTO lf (id, aa) VALUES (1, '{x}')", "22P02", "invalid input syntax for type integer").await;
    ok(&pool, "INSERT INTO lf (id, aa) VALUES (2, '{02}')").await;
    assert_eq!(column(&pool, "SELECT aa FROM lf").await, ["{2}"]);
    // A column added under a dropped one's name has its own type only.
    ok(&pool, "ALTER TABLE lf DROP COLUMN aa").await;
    ok(&pool, "ALTER TABLE lf ADD COLUMN aa TEXT[]").await;
    ok(&pool, "INSERT INTO lf (id, aa) VALUES (3, '{x}')").await;
    // An untyped array column with rows is not read again as another type
    // without `USING`, as in PostgreSQL.
    refused(&pool, "ALTER TABLE lf ALTER COLUMN aa TYPE INT[]", "42804", "cannot be cast automatically").await;
    ok(&pool, "INSERT INTO lf (id, aa) VALUES (4, '{y}')").await;
    // On an empty table the element type changes with the declaration.
    ok(&pool, "CREATE TABLE em (x INT[])").await;
    ok(&pool, "ALTER TABLE em ALTER COLUMN x TYPE BIGINT[]").await;
    refused(&pool, "INSERT INTO em VALUES ('{1.5}')", "22P02", "invalid input syntax for type bigint").await;
    ok(&pool, "ALTER TABLE em ALTER COLUMN x TYPE TEXT[]").await;
    ok(&pool, "INSERT INTO em VALUES ('{1.5}')").await;
    // A typed column holding rows is refused rather than relabelled.
    ok(&pool, "CREATE TABLE fu (x INT[])").await;
    ok(&pool, "INSERT INTO fu VALUES ('{3}')").await;
    refused(&pool, "ALTER TABLE fu ALTER COLUMN x TYPE TEXT[]", "0A000", "not supported yet").await;
    refused(&pool, "INSERT INTO fu VALUES ('{y}')", "22P02", "invalid input syntax for type integer").await;
}

#[tokio::test(flavor = "multi_thread")]
async fn create_table_as_keeps_the_element_type() {
    let (_s, pool) = server().await;
    ok(&pool, "CREATE TABLE src (x BIGINT[])").await;
    ok(&pool, "CREATE TABLE empty_copy AS SELECT x FROM src").await;
    refused(&pool, "INSERT INTO empty_copy VALUES ('{z}')", "22P02", "invalid input syntax for type bigint").await;
    ok(&pool, "INSERT INTO src VALUES ('{10}'), ('{9}')").await;
    ok(&pool, "CREATE TABLE full_copy AS SELECT x FROM src").await;
    assert_eq!(column(&pool, "SELECT x FROM full_copy ORDER BY x").await, ["{9}", "{10}"]);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_renamed_column_keeps_its_precision() {
    let (_s, pool) = server().await;
    ok(&pool, "CREATE TABLE pr (ts TIMESTAMP(0))").await;
    ok(&pool, "ALTER TABLE pr RENAME COLUMN ts TO t2").await;
    ok(&pool, "INSERT INTO pr VALUES ('2020-01-01 00:00:05.7')").await;
    assert_eq!(column(&pool, "SELECT t2 FROM pr").await, ["2020-01-01 00:00:06"]);
}
