//! `ARRAY[…]`, the array constructor, against PostgreSQL 17's answers.
//!
//! The select-list splitters cut it at its commas and the evaluator had no
//! constructor, so every use failed: `SELECT ARRAY[a, b]` with "Column
//! 'ARRAY[a' not found", `array_length(ARRAY[a, b], 1)` and `INSERT … VALUES
//! (ARRAY[3, 4])` likewise.

use rasterizeddb_compat::TestServer;
use sqlx::{AssertSqlSafe, Row};

async fn server() -> (TestServer, sqlx::PgPool) {
    let server = TestServer::start().await.expect("server should start");
    let pool = server.connect_sqlx().await.expect("should connect");
    sqlx::raw_sql("CREATE TABLE sp (a INT, b INT, c TEXT); INSERT INTO sp VALUES (1, 2, 'x'), (3, 4, 'y');")
        .execute(&pool)
        .await
        .expect("setup");
    (server, pool)
}

/// Each row's columns as text, over the simple protocol.
async fn rows(pool: &sqlx::PgPool, sql: &str) -> Vec<String> {
    let rows = sqlx::raw_sql(AssertSqlSafe(sql.to_string()))
        .fetch_all(pool)
        .await
        .unwrap_or_else(|e| panic!("`{sql}` should succeed: {e}"));
    rows.iter()
        .map(|row| {
            (0..row.len())
                .map(|i| row.try_get_unchecked::<Option<String>, _>(i).expect("text").unwrap_or_else(|| "NULL".into()))
                .collect::<Vec<_>>()
                .join("|")
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn the_constructor_builds_an_array_wherever_a_value_goes() {
    let (_s, pool) = server().await;
    assert_eq!(rows(&pool, "SELECT ARRAY[a, b] FROM sp ORDER BY a").await, ["{1,2}", "{3,4}"]);
    assert_eq!(rows(&pool, "SELECT ARRAY[a, b] AS ab, c FROM sp ORDER BY a").await, ["{1,2}|x", "{3,4}|y"]);
    assert_eq!(rows(&pool, "SELECT c, ARRAY[a, b, 7] FROM sp ORDER BY a").await, ["x|{1,2,7}", "y|{3,4,7}"]);
    assert_eq!(rows(&pool, "SELECT ARRAY[1,2,3]").await, ["{1,2,3}"]);
    assert_eq!(rows(&pool, "SELECT ARRAY[1.5, 2]").await, ["{1.5,2}"]);
    assert_eq!(rows(&pool, "SELECT ARRAY['p', c] FROM sp ORDER BY a").await, ["{p,x}", "{p,y}"]);
    assert_eq!(rows(&pool, "SELECT ARRAY[a, NULL, b] FROM sp ORDER BY a").await, ["{1,NULL,2}", "{3,NULL,4}"]);
    assert_eq!(
        rows(&pool, "SELECT ARRAY[c, 'q\"r', 'a b'] FROM sp ORDER BY a").await,
        ["{x,\"q\\\"r\",\"a b\"}", "{y,\"q\\\"r\",\"a b\"}"]
    );
    assert_eq!(rows(&pool, "SELECT array_length(ARRAY[a, b], 1) FROM sp ORDER BY a").await, ["2", "2"]);
    assert_eq!(rows(&pool, "SELECT ARRAY[a, b] = ARRAY[1, 2] FROM sp ORDER BY a").await, ["t", "f"]);
    assert_eq!(rows(&pool, "SELECT ARRAY[]::int[]").await, ["{}"]);
    sqlx::raw_sql("CREATE TABLE ia (x INT[]); INSERT INTO ia VALUES (ARRAY[3, 04]);").execute(&pool).await.expect("insert");
    assert_eq!(rows(&pool, "SELECT x FROM ia").await, ["{3,4}"]);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_constructed_array_orders_as_its_elements_type() {
    let (_s, pool) = server().await;
    sqlx::raw_sql("INSERT INTO sp VALUES (10, 0, 'z')").execute(&pool).await.expect("insert");
    assert_eq!(
        rows(&pool, "SELECT x FROM (SELECT ARRAY[a, b] AS x FROM sp) s ORDER BY x DESC").await,
        ["{10,0}", "{3,4}", "{1,2}"]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn an_unnamed_constructor_is_called_array() {
    let (_s, pool) = server().await;
    let row = sqlx::query("SELECT ARRAY[a, b] FROM sp WHERE a = 1").fetch_one(&pool).await.expect("select");
    use sqlx::Column;
    assert_eq!(row.columns()[0].name(), "array");
}

/// `||` with an array on either side concatenates, appends or prepends, as
/// PostgreSQL's array_cat, array_append and array_prepend. It went down the
/// text path, which read each array's stored bytes as an empty string, so
/// `ARRAY[1] || ARRAY[2]` was '' with no error. A NULL array gives the other
/// side, a NULL element is appended, and an untyped literal is an array
/// literal (refused when it is not one). PostgreSQL 17's values.
#[tokio::test(flavor = "multi_thread")]
async fn concatenation_joins_arrays_and_elements() {
    let (_server, pool) = server().await;
    for (sql, want) in [
        ("SELECT ARRAY[a] || ARRAY[b] FROM sp ORDER BY a", vec!["{1,2}", "{3,4}"]),
        ("SELECT ARRAY[c] || ARRAY['z', NULL] FROM sp ORDER BY a", vec!["{x,z,NULL}", "{y,z,NULL}"]),
        ("SELECT ARRAY[a, b] || a FROM sp ORDER BY a", vec!["{1,2,1}", "{3,4,3}"]),
        ("SELECT b || ARRAY[a] FROM sp ORDER BY a", vec!["{2,1}", "{4,3}"]),
        ("SELECT ARRAY[1] || '{2,3}'", vec!["{1,2,3}"]),
        ("SELECT ARRAY[1] || NULL", vec!["{1}"]),
        ("SELECT NULL::int[] || 1", vec!["{1}"]),
        ("SELECT ARRAY[1] || NULL::int", vec!["{1,NULL}"]),
        ("SELECT ARRAY[1] || ARRAY[2.5]", vec!["{1,2.5}"]),
        ("SELECT '{}'::int[] || ARRAY[7]", vec!["{7}"]),
        ("SELECT 'x' || c FROM sp ORDER BY a", vec!["xx", "xy"]),
    ] {
        assert_eq!(rows(&pool, sql).await, want, "{sql}");
    }
    let e = sqlx::raw_sql("SELECT ARRAY['a'] || 'b'").fetch_all(&pool).await.expect_err("'b' is no array literal");
    assert!(e.to_string().contains("malformed array literal"), "{e}");
}

/// `array_append`, `array_prepend` and `array_cat`, which were "not
/// supported yet": as `||` concatenates, except that a quoted element is an
/// element here, not an array literal, and a NULL array is the empty one.
/// PostgreSQL 17's values.
#[tokio::test(flavor = "multi_thread")]
async fn append_prepend_and_cat_build_arrays() {
    let (_server, pool) = server().await;
    for (sql, want) in [
        ("SELECT array_append(ARRAY[a], b) FROM sp ORDER BY a", vec!["{1,2}", "{3,4}"]),
        ("SELECT array_prepend(b, ARRAY[a]) FROM sp ORDER BY a", vec!["{2,1}", "{4,3}"]),
        ("SELECT array_cat(ARRAY[c], ARRAY['z']) FROM sp ORDER BY a", vec!["{x,z}", "{y,z}"]),
        ("SELECT array_append(ARRAY['a'], 'b')", vec!["{a,b}"]),
        ("SELECT array_prepend('z', ARRAY['a'])", vec!["{z,a}"]),
        ("SELECT array_append(NULL::int[], 5)", vec!["{5}"]),
        ("SELECT array_append(ARRAY['x'], NULL)", vec!["{x,NULL}"]),
        ("SELECT array_cat(NULL::int[], NULL::int[])", vec!["NULL"]),
        ("SELECT array_cat(ARRAY[1, 2], NULL)", vec!["{1,2}"]),
    ] {
        assert_eq!(rows(&pool, sql).await, want, "{sql}");
    }
}
