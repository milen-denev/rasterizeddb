//! A computed item in a join's SELECT list must appear in the result.
//!
//! `SELECT t.id, COALESCE(s.a, t.b) AS eff FROM t JOIN s ON …` returned a row
//! with no `eff` column at all — "no column found for name: eff". The value is
//! not wrong, it is absent, and a client asking for it by name gets an error
//! about its own query rather than anything naming the cause.

use rasterizeddb_compat::TestServer;
use sqlx::{Executor, Row};

async fn server() -> (TestServer, sqlx::PgPool) {
    let server = TestServer::start().await.expect("server should start");
    let pool = server.connect_sqlx().await.expect("should connect");
    (server, pool)
}

async fn setup(pool: &sqlx::PgPool) {
    pool.execute(sqlx::raw_sql(
        "CREATE TABLE t (id INT PRIMARY KEY, b INT NOT NULL, name TEXT NOT NULL)",
    ))
    .await
    .expect("t");
    pool.execute(sqlx::raw_sql("CREATE TABLE s (id INT PRIMARY KEY, t_id INT NOT NULL, a INT)"))
        .await
        .expect("s");
    pool.execute(sqlx::raw_sql("INSERT INTO t (id, b, name) VALUES (1, 10, 'one')"))
        .await
        .expect("t1");
    pool.execute(sqlx::raw_sql("INSERT INTO t (id, b, name) VALUES (2, 20, 'two')"))
        .await
        .expect("t2");
    pool.execute(sqlx::raw_sql("INSERT INTO s (id, t_id, a) VALUES (1, 1, 99)"))
        .await
        .expect("s1");
    pool.execute(sqlx::raw_sql("INSERT INTO s (id, t_id, a) VALUES (2, 2, NULL)"))
        .await
        .expect("s2");
}

#[tokio::test]
async fn a_computed_item_is_returned_from_a_join() {
    let (_s, pool) = server().await;
    setup(&pool).await;

    let rows = sqlx::query(
        "SELECT t.id AS id, COALESCE(s.a, t.b) AS eff FROM t JOIN s ON s.t_id = t.id ORDER BY t.id",
    )
    .fetch_all(&pool)
    .await
    .expect("select with a computed item");

    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<i32, _>("id"), 1);
    assert_eq!(
        rows[0].try_get::<i32, _>("eff").expect("eff must be in the result"),
        99,
        "row 1 takes s.a"
    );
    assert_eq!(
        rows[1].try_get::<i32, _>("eff").expect("eff must be in the result"),
        20,
        "row 2 falls through to t.b"
    );
}

/// Arithmetic, not just a function — same question, different item kind.
#[tokio::test]
async fn an_arithmetic_item_is_returned_from_a_join() {
    let (_s, pool) = server().await;
    setup(&pool).await;

    let row = sqlx::query(
        "SELECT t.id AS id, (t.b + 1) AS plus FROM t JOIN s ON s.t_id = t.id WHERE t.id = 1",
    )
    .fetch_one(&pool)
    .await
    .expect("select with arithmetic");
    assert_eq!(row.try_get::<i32, _>("plus").expect("plus must be in the result"), 11);
}

/// Every computed shape must come back with its column *and* the right value.
///
/// This replaces a test that asserted these were refused. The refusal was the
/// previous fix for a worse bug — the item was set aside for an aggregate stage
/// that never formed, so the statement succeeded with the column simply absent,
/// which reaches a client as "no column found for name: eff". They are
/// evaluated over the combined row now, so the original concern is checked
/// directly: the column is present and holds what it should.
#[tokio::test]
async fn every_computed_shape_is_evaluated_not_dropped() {
    let (_s, pool) = server().await;
    setup(&pool).await;

    // Row 1 takes `s.a` (99); row 2's `s.a` is NULL so it falls through to
    // `t.b` (20) — the fallback, not the emptiness.
    let rows = sqlx::query(
        "SELECT t.id AS id, COALESCE(s.a, t.b) AS eff FROM t JOIN s ON s.t_id = t.id ORDER BY t.id",
    )
    .fetch_all(&pool)
    .await
    .expect("COALESCE over a join");
    assert_eq!(
        rows.iter().map(|r| r.get::<i32, _>("eff")).collect::<Vec<_>>(),
        vec![99, 20]
    );

    let rows = sqlx::query(
        "SELECT t.id AS id, (t.b + 1) AS plus FROM t JOIN s ON s.t_id = t.id ORDER BY t.id",
    )
    .fetch_all(&pool)
    .await
    .expect("arithmetic over a join");
    assert_eq!(
        rows.iter().map(|r| r.get::<i32, _>("plus")).collect::<Vec<_>>(),
        vec![11, 21]
    );

    let rows = sqlx::query(
        "SELECT t.id AS id, UPPER(t.name) AS up FROM t JOIN s ON s.t_id = t.id ORDER BY t.id",
    )
    .fetch_all(&pool)
    .await
    .expect("a text function over a join");
    assert_eq!(
        rows.iter().map(|r| r.get::<String, _>("up")).collect::<Vec<_>>(),
        vec!["ONE".to_string(), "TWO".to_string()]
    );

    // A plain column list still works, so the guard has not widened.
    let rows = sqlx::query("SELECT t.id AS id, s.a AS a FROM t JOIN s ON s.t_id = t.id ORDER BY t.id")
        .fetch_all(&pool)
        .await
        .expect("plain columns still work");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<i32, _>("id"), 1);
    assert_eq!(rows[0].get::<Option<i32>, _>("a"), Some(99));
    assert_eq!(rows[1].get::<Option<i32>, _>("a"), None);
}
