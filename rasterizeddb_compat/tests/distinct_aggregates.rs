//! Aggregates over DISTINCT values: `COUNT(DISTINCT x)` and its kin.
//!
//! They were read as a column named `DISTINCT x` and refused as 42601. Every
//! expected answer is PostgreSQL 17's on the same rows: NULL is never counted,
//! a group whose values are all NULL counts 0, and a shape that cannot be
//! answered is refused as unsupported, 0A000, never answered wrongly.

use rasterizeddb_compat::TestServer;
use sqlx::{Executor, Row};

async fn setup() -> (TestServer, sqlx::PgPool) {
    let server = TestServer::start().await.expect("server");
    let pool = server.connect_sqlx().await.expect("connect");
    pool.execute(sqlx::raw_sql(
        "CREATE TABLE t (id INT NOT NULL, g INT, x INT);
         INSERT INTO t VALUES (1, 1, 10), (2, 1, 10), (3, 1, 20), (4, 2, NULL), (5, 2, 30), (6, 2, 30), (7, 3, NULL);",
    ))
    .await
    .expect("schema");
    (server, pool)
}

#[tokio::test(flavor = "multi_thread")]
async fn distinct_aggregates_answer_as_postgresql_does() {
    let (_server, pool) = setup().await;

    // Through the extended protocol, as drivers send it.
    let count: i64 = sqlx::query("SELECT COUNT(DISTINCT x) FROM t").fetch_one(&pool).await.expect("count").get(0);
    assert_eq!(count, 3);
    let count: i64 = sqlx::query("SELECT COUNT(DISTINCT x) FROM t WHERE id > $1")
        .bind(3)
        .fetch_one(&pool)
        .await
        .expect("count with a parameter")
        .get(0);
    assert_eq!(count, 1);
    let sum: i64 = sqlx::query("SELECT SUM(DISTINCT x) FROM t").fetch_one(&pool).await.expect("sum").get(0);
    assert_eq!(sum, 60);

    let grouped: Vec<(i32, i64)> = sqlx::query("SELECT g, COUNT(DISTINCT x) AS n FROM t GROUP BY g ORDER BY g")
        .fetch_all(&pool)
        .await
        .expect("grouped")
        .iter()
        .map(|r| (r.get(0), r.get(1)))
        .collect();
    assert_eq!(grouped, vec![(1, 2), (2, 1), (3, 0)]);

    // And through the simple protocol.
    let rows = pool.fetch_all(sqlx::raw_sql("SELECT COUNT(DISTINCT x) FROM t")).await.expect("simple");
    assert_eq!(rows[0].get::<i64, _>(0), 3);
}

/// Several aggregates, over DISTINCT values of different expressions and
/// beside plain ones, in one statement: the fold keeps a set per item.
#[tokio::test(flavor = "multi_thread")]
async fn distinct_and_plain_aggregates_mix() {
    let (_server, pool) = setup().await;
    let row = sqlx::query("SELECT COUNT(DISTINCT g), COUNT(DISTINCT x), COUNT(*), COUNT(x), SUM(DISTINCT x), SUM(x) FROM t")
        .fetch_one(&pool)
        .await
        .expect("mixed");
    let got: (i64, i64, i64, i64, i64, i64) = (row.get(0), row.get(1), row.get(2), row.get(3), row.get(4), row.get(5));
    assert_eq!(got, (3, 3, 7, 5, 60, 100));

    let grouped: Vec<(i32, i64, i64)> =
        sqlx::query("SELECT g, COUNT(DISTINCT x), COUNT(*) FROM t GROUP BY g ORDER BY g")
            .fetch_all(&pool)
            .await
            .expect("grouped mix")
            .iter()
            .map(|r| (r.get(0), r.get(1), r.get(2)))
            .collect();
    assert_eq!(grouped, vec![(1, 2, 3), (2, 1, 3), (3, 0, 1)]);

    // HAVING over the same aggregate the select list holds.
    let having: Vec<(i32, i64)> =
        sqlx::query("SELECT g, COUNT(DISTINCT x) FROM t GROUP BY g HAVING COUNT(DISTINCT x) > 1 ORDER BY g")
            .fetch_all(&pool)
            .await
            .expect("having")
            .iter()
            .map(|r| (r.get(0), r.get(1)))
            .collect();
    assert_eq!(having, vec![(1, 2)]);
    // And a DISTINCT aggregate that only HAVING names: answered, or refused
    // as unsupported, never answered as though DISTINCT were not there.
    match sqlx::query("SELECT g FROM t GROUP BY g HAVING COUNT(DISTINCT x) > 1 ORDER BY g").fetch_all(&pool).await {
        Ok(rows) => assert_eq!(rows.iter().map(|r| r.get::<i32, _>(0)).collect::<Vec<_>>(), vec![1]),
        Err(e) => assert_eq!(e.as_database_error().and_then(|d| d.code()).as_deref(), Some("0A000"), "{e}"),
    }

    // `ALL` is the default spelled out.
    let all: i64 = sqlx::query("SELECT COUNT(ALL x) FROM t").fetch_one(&pool).await.expect("all").get(0);
    assert_eq!(all, 5);

    // -0.0 and 0.0 are one value.
    pool.execute(sqlx::raw_sql("CREATE TABLE f (v DOUBLE PRECISION); INSERT INTO f VALUES (0.0), (-0.0), (1.5);"))
        .await
        .expect("floats");
    let n: i64 = sqlx::query("SELECT COUNT(DISTINCT v) FROM f").fetch_one(&pool).await.expect("floats").get(0);
    assert_eq!(n, 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn shapes_it_cannot_answer_are_refused_as_unsupported() {
    let (_server, pool) = setup().await;
    for sql in ["SELECT COUNT(DISTINCT x) + 1 FROM t"] {
        let e = sqlx::query(sql).fetch_all(&pool).await.expect_err(sql);
        assert_eq!(e.as_database_error().and_then(|d| d.code()).as_deref(), Some("0A000"), "{sql}: {e}");
    }
}

/// A column is described under its alias, which the rewrite above depends on
/// and every driver reading by name does too: `SELECT x AS v` was described
/// as `x` over the extended protocol, and a derived table that renamed a
/// column was described as having no columns.
#[tokio::test(flavor = "multi_thread")]
async fn a_column_is_described_under_its_alias() {
    use sqlx::Column;
    let (_server, pool) = setup().await;
    let names = |row: &sqlx::postgres::PgRow| row.columns().iter().map(|c| c.name().to_string()).collect::<Vec<_>>();

    let row = sqlx::query("SELECT x AS v, id FROM t WHERE id = 1").fetch_one(&pool).await.expect("alias");
    assert_eq!(names(&row), ["v", "id"]);
    assert_eq!(row.get::<i32, _>("v"), 10);

    let row = sqlx::query("SELECT id AS b, x AS a FROM t WHERE id = 3").fetch_one(&pool).await.expect("reordered");
    assert_eq!(names(&row), ["b", "a"]);
    assert_eq!((row.get::<i32, _>("b"), row.get::<i32, _>("a")), (3, 20));

    let row = sqlx::query("SELECT x AS first, x AS second FROM t WHERE id = 1").fetch_one(&pool).await.expect("twice");
    assert_eq!(names(&row), ["first", "second"]);

    let row = sqlx::query("SELECT SUM(v) FROM (SELECT x AS v FROM t) d").fetch_one(&pool).await.expect("derived");
    assert_eq!(names(&row), ["sum"]);
    assert_eq!(row.get::<i64, _>(0), 100);
}
