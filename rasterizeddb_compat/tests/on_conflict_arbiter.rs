//! An ON CONFLICT target must name a unique key exactly, as PostgreSQL infers
//! its arbiter; anything else is 42P10, never a guess.
//!
//! `ON CONFLICT (tenant_id) DO UPDATE` over a key on `(tenant_id, grp, id)`
//! used to update every row of the tenant. Found by rasterizeddb-eb.
//! Expected answers are PostgreSQL 17's.

use rasterizeddb_compat::TestServer;
use sqlx::{AssertSqlSafe, Executor, Row};

async fn refused(pool: &sqlx::PgPool, sql: &str, code: &str) {
    match sqlx::query(AssertSqlSafe(sql)).execute(pool).await {
        Ok(done) => panic!("{sql}: PostgreSQL refuses this with {code}, but it affected {} rows", done.rows_affected()),
        Err(e) => assert_eq!(e.as_database_error().and_then(|d| d.code()).as_deref(), Some(code), "{sql}: {e}"),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_target_that_is_not_exactly_a_key_is_refused() {
    let server = TestServer::start().await.expect("server");
    let pool = server.connect_sqlx().await.expect("connect");
    pool.execute(sqlx::raw_sql(
        "CREATE TABLE t (tenant_id INT NOT NULL, grp INT NOT NULL, id INT NOT NULL, n INT, PRIMARY KEY (tenant_id, grp, id));
         INSERT INTO t VALUES (1, 1, 1, 10), (1, 1, 2, 20), (1, 2, 3, 30), (2, 1, 1, 40);",
    ))
    .await
    .expect("schema");

    // A subset of the key, a superset, and a column in no key.
    for target in ["(tenant_id)", "(tenant_id, grp)", "(tenant_id, grp, id, n)", "(n)"] {
        refused(&pool, &format!("INSERT INTO t VALUES (1, 1, 1, 99) ON CONFLICT {target} DO UPDATE SET n = EXCLUDED.n"), "42P10").await;
        refused(&pool, &format!("INSERT INTO t VALUES (1, 1, 1, 99) ON CONFLICT {target} DO NOTHING"), "42P10").await;
    }
    // Nothing changed.
    let ns: Vec<i32> = sqlx::query("SELECT n FROM t ORDER BY tenant_id, grp, id")
        .fetch_all(&pool)
        .await
        .expect("read")
        .iter()
        .map(|r| r.get(0))
        .collect();
    assert_eq!(ns, vec![10, 20, 30, 40]);

    // The exact key, in any order, updates the one row.
    pool.execute("INSERT INTO t VALUES (1, 1, 1, 99) ON CONFLICT (id, tenant_id, grp) DO UPDATE SET n = EXCLUDED.n")
        .await
        .expect("exact key");
    let ns: Vec<i32> = sqlx::query("SELECT n FROM t ORDER BY tenant_id, grp, id")
        .fetch_all(&pool)
        .await
        .expect("read")
        .iter()
        .map(|r| r.get(0))
        .collect();
    assert_eq!(ns, vec![99, 20, 30, 40]);
}

#[tokio::test(flavor = "multi_thread")]
async fn single_column_keys_are_inferred_by_their_column() {
    let server = TestServer::start().await.expect("server");
    let pool = server.connect_sqlx().await.expect("connect");
    pool.execute(sqlx::raw_sql(
        "CREATE TABLE u (id INT PRIMARY KEY, email TEXT UNIQUE, n INT);
         INSERT INTO u VALUES (1, 'a', 1);",
    ))
    .await
    .expect("schema");
    pool.execute("INSERT INTO u VALUES (1, 'b', 2) ON CONFLICT (id) DO UPDATE SET n = EXCLUDED.n").await.expect("pk");
    pool.execute("INSERT INTO u VALUES (5, 'a', 3) ON CONFLICT (email) DO UPDATE SET n = EXCLUDED.n").await.expect("unique column");
    let n: i32 = sqlx::query("SELECT n FROM u WHERE id = 1").fetch_one(&pool).await.expect("read").get(0);
    assert_eq!(n, 3);
    refused(&pool, "INSERT INTO u VALUES (1, 'z', 4) ON CONFLICT (id, email) DO NOTHING", "42P10").await;
}

/// One statement may not update one row twice: 21000, and nothing written.
#[tokio::test(flavor = "multi_thread")]
async fn a_key_proposed_twice_in_one_statement_is_refused() {
    let server = TestServer::start().await.expect("server");
    let pool = server.connect_sqlx().await.expect("connect");
    pool.execute(sqlx::raw_sql("CREATE TABLE k (id INT PRIMARY KEY, n INT); INSERT INTO k VALUES (1, 1);"))
        .await
        .expect("schema");
    // Twice for an existing row, and twice for a new one.
    for sql in [
        "INSERT INTO k VALUES (1, 10), (1, 20) ON CONFLICT (id) DO UPDATE SET n = EXCLUDED.n",
        "INSERT INTO k VALUES (7, 10), (8, 0), (7, 20) ON CONFLICT (id) DO UPDATE SET n = EXCLUDED.n",
    ] {
        refused(&pool, sql, "21000").await;
    }
    let rows: Vec<(i32, i32)> = sqlx::query("SELECT id, n FROM k ORDER BY id")
        .fetch_all(&pool)
        .await
        .expect("read")
        .iter()
        .map(|r| (r.get(0), r.get(1)))
        .collect();
    assert_eq!(rows, vec![(1, 1)], "a refused statement wrote rows");
    // Distinct keys are fine, and DO NOTHING never counts as a second update.
    pool.execute("INSERT INTO k VALUES (1, 5), (2, 6) ON CONFLICT (id) DO UPDATE SET n = EXCLUDED.n").await.expect("distinct");
    pool.execute("INSERT INTO k VALUES (3, 1), (3, 2) ON CONFLICT (id) DO NOTHING").await.expect("do nothing");
}
