//! `EXISTS` / `NOT EXISTS` whose body carries `ORDER BY`, `LIMIT` or `OFFSET`.
//!
//! Django's `Exists()` appends `LIMIT 1` to every existence subquery. The
//! correlated rewrite (`EXISTS (… WHERE t.a = o.b …)` -> `o.b IN (SELECT a …)`)
//! used to carry that LIMIT into the IN list, which then held ONE value, so
//! every outer row matching any other value was silently dropped. With the
//! LIMIT directly after the correlation the outer reference itself became
//! `o.b LIMIT 1`.
//!
//! Semantics checked against PostgreSQL:
//!   * ORDER BY never changes whether a row exists;
//!   * LIMIT n (n >= 1) never changes it either;
//!   * LIMIT 0 makes EXISTS false (NOT EXISTS true) for every row;
//!   * OFFSET k means "at least k + 1 rows".

use rasterizeddb_compat::TestServer;
use sqlx::AssertSqlSafe;
use sqlx::{Executor, Row};

async fn server() -> (TestServer, sqlx::PgPool) {
    let server = TestServer::start().await.expect("server should start");
    let pool = server.connect_sqlx().await.expect("should connect");
    (server, pool)
}

async fn run(pool: &sqlx::PgPool, sql: &str) -> Result<(), String> {
    pool.execute(sqlx::raw_sql(AssertSqlSafe(sql)))
        .await
        .map(|_| ())
        .map_err(|e| e.to_string().lines().next().unwrap_or("").to_string())
}

async fn ids(pool: &sqlx::PgPool, sql: &str) -> Result<Vec<i32>, String> {
    let rows = pool
        .fetch_all(sqlx::raw_sql(AssertSqlSafe(sql)))
        .await
        .map_err(|e| e.to_string())?;
    let mut out: Vec<i32> = rows
        .iter()
        .map(|r| r.try_get::<i32, _>(0).expect("an INT id"))
        .collect();
    out.sort();
    Ok(out)
}

/// `o` has ids 1..=4 with `b = id`; `t` holds a = 1 (flag 1), a = 2 (flag 1
/// and flag 0), a = 3 (flag 0). So with `flag = 1`, outer rows 1 and 2 match.
async fn seed(pool: &sqlx::PgPool) {
    run(pool, "CREATE TABLE o (id INT PRIMARY KEY, b INT NOT NULL)")
        .await
        .expect("create o");
    run(pool, "CREATE TABLE t (id INT PRIMARY KEY, a INT NOT NULL, flag INT NOT NULL)")
        .await
        .expect("create t");
    for id in 1..=4 {
        run(pool, &format!("INSERT INTO o (id, b) VALUES ({id}, {id})"))
            .await
            .expect("insert o");
    }
    for (id, a, flag) in [(1, 1, 1), (2, 2, 1), (3, 2, 0), (4, 3, 0)] {
        run(
            pool,
            &format!("INSERT INTO t (id, a, flag) VALUES ({id}, {a}, {flag})"),
        )
        .await
        .expect("insert t");
    }
}

#[tokio::test]
async fn correlated_exists_with_limit_one_keeps_every_matching_row() {
    let (_s, pool) = server().await;
    seed(&pool).await;

    // Django's shape: the LIMIT trails another conjunct.
    assert_eq!(
        ids(&pool, "SELECT id FROM o WHERE EXISTS (SELECT 1 FROM t WHERE t.a = o.b AND t.flag = 1 LIMIT 1)").await,
        Ok(vec![1, 2]),
    );
    assert_eq!(
        ids(&pool, "SELECT id FROM o WHERE NOT EXISTS (SELECT 1 FROM t WHERE t.a = o.b AND t.flag = 1 LIMIT 1)").await,
        Ok(vec![3, 4]),
    );
    // The LIMIT directly after the correlation used to become part of the
    // outer reference (`o.b LIMIT 1`).
    assert_eq!(
        ids(&pool, "SELECT id FROM o WHERE EXISTS (SELECT 1 FROM t WHERE t.a = o.b LIMIT 1)").await,
        Ok(vec![1, 2, 3]),
    );
    assert_eq!(
        ids(&pool, "SELECT id FROM o WHERE NOT EXISTS (SELECT 1 FROM t WHERE t.a = o.b LIMIT 1)").await,
        Ok(vec![4]),
    );
    // Newlines rather than spaces, as an ORM may emit them.
    assert_eq!(
        ids(&pool, "SELECT id FROM o WHERE EXISTS (SELECT 1 FROM t\nWHERE t.a = o.b\nLIMIT 1)").await,
        Ok(vec![1, 2, 3]),
    );
}

#[tokio::test]
async fn correlated_exists_with_order_by_and_limit() {
    let (_s, pool) = server().await;
    seed(&pool).await;

    assert_eq!(
        ids(&pool, "SELECT id FROM o WHERE EXISTS (SELECT 1 FROM t WHERE t.a = o.b ORDER BY t.id DESC LIMIT 1)").await,
        Ok(vec![1, 2, 3]),
    );
    assert_eq!(
        ids(&pool, "SELECT id FROM o WHERE NOT EXISTS (SELECT 1 FROM t WHERE o.b = t.a AND t.flag = 0 ORDER BY t.id LIMIT 1)").await,
        Ok(vec![1, 4]),
    );
    // ORDER BY alone.
    assert_eq!(
        ids(&pool, "SELECT id FROM o WHERE EXISTS (SELECT 1 FROM t WHERE t.a = o.b AND t.flag = 1 ORDER BY t.id)").await,
        Ok(vec![1, 2]),
    );
    // A LIMIT inside a nested group belongs to that group, not to the EXISTS.
    assert_eq!(
        ids(&pool, "SELECT id FROM o WHERE EXISTS (SELECT 1 FROM t WHERE t.a = o.b AND t.flag IN (SELECT flag FROM t WHERE flag = 1 LIMIT 1) LIMIT 1)").await,
        Ok(vec![1, 2]),
    );
    // A keyword inside a string literal is data.
    assert_eq!(
        ids(&pool, "SELECT id FROM o WHERE EXISTS (SELECT 1 FROM t WHERE t.a = o.b AND 'x LIMIT 0' = 'x LIMIT 0' LIMIT 1)").await,
        Ok(vec![1, 2, 3]),
    );
}

#[tokio::test]
async fn exists_with_limit_zero_is_never_true() {
    let (_s, pool) = server().await;
    seed(&pool).await;

    // Correlated.
    assert_eq!(
        ids(&pool, "SELECT id FROM o WHERE EXISTS (SELECT 1 FROM t WHERE t.a = o.b LIMIT 0)").await,
        Ok(vec![]),
    );
    assert_eq!(
        ids(&pool, "SELECT id FROM o WHERE NOT EXISTS (SELECT 1 FROM t WHERE t.a = o.b LIMIT 0)").await,
        Ok(vec![1, 2, 3, 4]),
    );
    // Uncorrelated.
    assert_eq!(
        ids(&pool, "SELECT id FROM o WHERE EXISTS (SELECT 1 FROM t LIMIT 0)").await,
        Ok(vec![]),
    );
    assert_eq!(
        ids(&pool, "SELECT id FROM o WHERE NOT EXISTS (SELECT 1 FROM t WHERE flag = 1 ORDER BY id LIMIT 0)").await,
        Ok(vec![1, 2, 3, 4]),
    );
}

#[tokio::test]
async fn uncorrelated_exists_with_limit_and_order() {
    let (_s, pool) = server().await;
    seed(&pool).await;

    assert_eq!(
        ids(&pool, "SELECT id FROM o WHERE EXISTS (SELECT 1 FROM t WHERE flag = 1 LIMIT 1)").await,
        Ok(vec![1, 2, 3, 4]),
    );
    assert_eq!(
        ids(&pool, "SELECT id FROM o WHERE EXISTS (SELECT 1 FROM t WHERE a = 99 ORDER BY id LIMIT 1)").await,
        Ok(vec![]),
    );
    assert_eq!(
        ids(&pool, "SELECT id FROM o WHERE NOT EXISTS (SELECT 1 FROM t WHERE a = 99 ORDER BY id DESC LIMIT 1)").await,
        Ok(vec![1, 2, 3, 4]),
    );
    assert_eq!(
        ids(&pool, "SELECT id FROM o WHERE NOT EXISTS (SELECT 1 FROM t ORDER BY id LIMIT 1)").await,
        Ok(vec![]),
    );
    // The seeding idiom.
    run(&pool, "INSERT INTO o (id, b) SELECT 9, 9 WHERE NOT EXISTS (SELECT 1 FROM o WHERE id = 9 LIMIT 1)")
        .await
        .expect("guarded insert");
    run(&pool, "INSERT INTO o (id, b) SELECT 9, 9 WHERE NOT EXISTS (SELECT 1 FROM o WHERE id = 9 LIMIT 1)")
        .await
        .expect("guarded insert, second time");
    assert_eq!(ids(&pool, "SELECT id FROM o WHERE id = 9").await, Ok(vec![9]));
}

#[tokio::test]
async fn exists_with_offset_means_more_than_offset_rows() {
    let (_s, pool) = server().await;
    seed(&pool).await;

    // Uncorrelated: t has four rows, two with flag = 1.
    assert_eq!(
        ids(&pool, "SELECT id FROM o WHERE EXISTS (SELECT 1 FROM t WHERE flag = 1 OFFSET 1)").await,
        Ok(vec![1, 2, 3, 4]),
    );
    assert_eq!(
        ids(&pool, "SELECT id FROM o WHERE EXISTS (SELECT 1 FROM t WHERE flag = 1 LIMIT 1 OFFSET 2)").await,
        Ok(vec![]),
    );
    assert_eq!(
        ids(&pool, "SELECT id FROM o WHERE NOT EXISTS (SELECT 1 FROM t WHERE flag = 1 ORDER BY id OFFSET 2)").await,
        Ok(vec![1, 2, 3, 4]),
    );
    // OFFSET 0 is no offset at all.
    assert_eq!(
        ids(&pool, "SELECT id FROM o WHERE EXISTS (SELECT 1 FROM t WHERE t.a = o.b LIMIT 1 OFFSET 0)").await,
        Ok(vec![1, 2, 3]),
    );
    // Correlated with a real offset: correct or refused, never wrong.
    match ids(&pool, "SELECT id FROM o WHERE EXISTS (SELECT 1 FROM t WHERE t.a = o.b OFFSET 1)").await {
        Ok(found) => assert_eq!(found, vec![2], "only a = 2 has more than one row"),
        Err(e) => assert!(e.contains("OFFSET"), "refusal should name OFFSET: {e}"),
    }
}

/// `NOT EXISTS` is two-valued; the `NOT IN` it is rewritten into is not. A NULL
/// on either side must not change which rows come back.
#[tokio::test]
async fn not_exists_is_not_poisoned_by_nulls() {
    let (_s, pool) = server().await;
    run(&pool, "CREATE TABLE no (id INT PRIMARY KEY, b INT)")
        .await
        .expect("create no");
    run(&pool, "CREATE TABLE nt (id INT PRIMARY KEY, a INT)")
        .await
        .expect("create nt");
    run(&pool, "INSERT INTO no (id, b) VALUES (1, 1)").await.expect("insert");
    run(&pool, "INSERT INTO no (id, b) VALUES (2, 2)").await.expect("insert");
    run(&pool, "INSERT INTO no (id, b) VALUES (3, NULL)").await.expect("insert");
    run(&pool, "INSERT INTO nt (id, a) VALUES (1, 1)").await.expect("insert");
    run(&pool, "INSERT INTO nt (id, a) VALUES (2, NULL)").await.expect("insert");

    assert_eq!(
        ids(&pool, "SELECT id FROM no WHERE EXISTS (SELECT 1 FROM nt WHERE nt.a = no.b)").await,
        Ok(vec![1]),
    );
    assert_eq!(
        ids(&pool, "SELECT id FROM no WHERE NOT EXISTS (SELECT 1 FROM nt WHERE nt.a = no.b)").await,
        Ok(vec![2, 3]),
    );
    assert_eq!(
        ids(&pool, "SELECT id FROM no WHERE NOT EXISTS (SELECT 1 FROM nt WHERE nt.a = no.b LIMIT 1)").await,
        Ok(vec![2, 3]),
    );
}

/// `IN (SELECT …)` over a column holding NULLs and duplicates, against
/// PostgreSQL's three-valued answers in a WHERE clause.
#[tokio::test]
async fn in_subquery_with_nulls_and_duplicates() {
    let (_s, pool) = server().await;
    run(&pool, "CREATE TABLE io (id INT PRIMARY KEY, b INT)").await.expect("create");
    run(&pool, "CREATE TABLE it (id INT PRIMARY KEY, a INT)").await.expect("create");
    for (id, b) in [(1, "1"), (2, "2"), (3, "NULL"), (4, "4")] {
        run(&pool, &format!("INSERT INTO io (id, b) VALUES ({id}, {b})")).await.expect("insert");
    }
    for (id, a) in [(1, "1"), (2, "1"), (3, "4"), (4, "NULL"), (5, "4")] {
        run(&pool, &format!("INSERT INTO it (id, a) VALUES ({id}, {a})")).await.expect("insert");
    }

    assert_eq!(ids(&pool, "SELECT id FROM io WHERE b IN (SELECT a FROM it)").await, Ok(vec![1, 4]));
    // A NULL in the list makes NOT IN unknown for every non-matching row.
    assert_eq!(ids(&pool, "SELECT id FROM io WHERE b NOT IN (SELECT a FROM it)").await, Ok(vec![]));
    // Without the NULL it is an ordinary anti-membership; a NULL b stays out.
    assert_eq!(
        ids(&pool, "SELECT id FROM io WHERE b NOT IN (SELECT a FROM it WHERE a IS NOT NULL)").await,
        Ok(vec![2]),
    );
    assert_eq!(
        ids(&pool, "SELECT id FROM io WHERE b IN (SELECT a FROM it ORDER BY a DESC)").await,
        Ok(vec![1, 4]),
    );
    // With a LIMIT the ORDER BY picks which rows count, so it must be kept.
    assert_eq!(
        ids(&pool, "SELECT id FROM io WHERE b IN (SELECT a FROM it WHERE a IS NOT NULL ORDER BY a DESC LIMIT 1)").await,
        Ok(vec![4]),
    );
    assert_eq!(
        ids(&pool, "SELECT id FROM io WHERE b IN (SELECT a FROM it WHERE a IS NOT NULL ORDER BY a LIMIT 1)").await,
        Ok(vec![1]),
    );

    // NOT EXISTS correlating on two columns, with NULLs on both sides.
    assert_eq!(
        ids(&pool, "SELECT id FROM io WHERE NOT EXISTS (SELECT 1 FROM it WHERE it.a = io.b AND it.id = io.id)").await,
        Ok(vec![2, 3, 4]),
    );
}

/// An aggregate without GROUP BY returns one row whatever matched, so a
/// correlated EXISTS over it holds for every outer row.
#[tokio::test]
async fn correlated_exists_over_an_aggregate_always_holds() {
    let (_s, pool) = server().await;
    seed(&pool).await;
    assert_eq!(
        ids(&pool, "SELECT id FROM o WHERE EXISTS (SELECT COUNT(*) FROM t WHERE t.a = o.b)").await,
        Ok(vec![1, 2, 3, 4]),
    );
    assert_eq!(
        ids(&pool, "SELECT id FROM o WHERE NOT EXISTS (SELECT MAX(t.id) FROM t WHERE t.a = o.b)").await,
        Ok(vec![]),
    );
}

/// A scalar subquery returning more than one row is still an error once the
/// scan is capped, and one returning one row still yields it.
#[tokio::test]
async fn scalar_subquery_keeps_its_cardinality_error() {
    let (_s, pool) = server().await;
    seed(&pool).await;
    let err = ids(&pool, "SELECT id FROM o WHERE b = (SELECT a FROM t WHERE flag = 1)").await;
    assert!(err.as_ref().is_err_and(|e| e.contains("at most one")), "{err:?}");
    assert_eq!(
        ids(&pool, "SELECT id FROM o WHERE b = (SELECT a FROM t WHERE id = 4)").await,
        Ok(vec![3]),
    );
    assert_eq!(
        ids(&pool, "SELECT id FROM o WHERE b = (SELECT a FROM t ORDER BY a DESC LIMIT 1)").await,
        Ok(vec![3]),
    );
}
