//! The statements pms ran while migrating onto a 1.0.0 server, each answered
//! as PostgreSQL 17 answers it (1.0.2).
//!
//! Every one failed before: a transaction dropping a child then its parent, a
//! correlated reference in an inner join's `ON` inside `EXISTS`, a window
//! beside such a subquery, `make_interval` over a column, a parenthesised
//! cast as a `DEFAULT`, an alias written without `AS`, and `SHOW TABLES`
//! naming a composite key.

use rasterizeddb_compat::TestServer;
use sqlx::{AssertSqlSafe, Row};

async fn server() -> (TestServer, sqlx::PgPool) {
    let server = TestServer::start().await.expect("server should start");
    let pool = server.connect_sqlx().await.expect("should connect");
    (server, pool)
}

async fn run(pool: &sqlx::PgPool, sql: &str) {
    sqlx::raw_sql(AssertSqlSafe(sql))
        .execute(pool)
        .await
        .unwrap_or_else(|e| panic!("`{sql}` failed: {e}"));
}

async fn ids(pool: &sqlx::PgPool, sql: &str) -> Vec<i32> {
    sqlx::query(AssertSqlSafe(sql))
        .fetch_all(pool)
        .await
        .unwrap_or_else(|e| panic!("`{sql}` failed: {e}"))
        .iter()
        .map(|r| r.try_get::<i32, _>(0).unwrap_or_else(|e| panic!("`{sql}` returned {e}")))
        .collect()
}

/// A migrator drops in key order inside one transaction.
#[tokio::test(flavor = "multi_thread")]
async fn a_transaction_drops_a_child_then_its_parent() {
    let (_s, pool) = server().await;
    run(&pool, "CREATE TABLE zz_parent (id uuid PRIMARY KEY)").await;
    run(&pool, "CREATE TABLE zz_child (id uuid PRIMARY KEY, p uuid REFERENCES zz_parent(id))").await;
    let mut tx = pool.begin().await.unwrap();
    sqlx::query("DROP TABLE IF EXISTS zz_child").execute(&mut *tx).await.expect("child");
    sqlx::query("DROP TABLE IF EXISTS zz_parent").execute(&mut *tx).await.expect("parent, its child gone");
    tx.commit().await.unwrap();
    let left: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM information_schema.tables WHERE table_name IN ('zz_parent', 'zz_child')",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(left, 0);

    // Without the child's drop first, still refused.
    run(&pool, "CREATE TABLE zz_p2 (id int PRIMARY KEY)").await;
    run(&pool, "CREATE TABLE zz_c2 (id int PRIMARY KEY, p int REFERENCES zz_p2(id))").await;
    let err = sqlx::query("DROP TABLE zz_p2").execute(&pool).await.expect_err("a child depends on it");
    assert!(err.to_string().contains("depend"), "{err}");
}

async fn swarm(pool: &sqlx::PgPool) {
    run(pool, "CREATE TABLE vynchat_swarm (id int PRIMARY KEY, sequential bool NOT NULL)").await;
    run(
        pool,
        "CREATE TABLE vynchat_swarm_task (id int PRIMARY KEY, swarm_id int, domain_id int, status text, \
         created_at int, task_index int)",
    )
    .await;
    run(pool, "INSERT INTO vynchat_swarm VALUES (1, true), (2, false)").await;
    run(
        pool,
        "INSERT INTO vynchat_swarm_task VALUES (10,1,1,'pending',1,0), (11,1,1,'pending',2,1), \
         (20,2,1,'pending',3,0), (21,2,1,'pending',4,1)",
    )
    .await;
}

/// pms's scheduler query: a correlation in an inner join's `ON` inside `NOT
/// EXISTS`, under a window, in a derived table, in a CTE. A task of a
/// sequential swarm waits for the tasks before it.
#[tokio::test(flavor = "multi_thread")]
async fn a_correlation_in_a_join_on_inside_exists_is_answered() {
    let (_s, pool) = server().await;
    swarm(&pool).await;
    let query = "WITH runnable AS (SELECT id FROM ( \
        SELECT id, ROW_NUMBER() OVER (PARTITION BY domain_id ORDER BY created_at, task_index) AS domain_rank \
        FROM vynchat_swarm_task task \
        WHERE status = 'pending' \
          AND NOT EXISTS (SELECT 1 FROM vynchat_swarm s \
               JOIN vynchat_swarm_task prior ON prior.swarm_id = s.id AND prior.task_index < task.task_index \
               WHERE s.id = task.swarm_id AND s.sequential) \
        ) ranked WHERE ranked.domain_rank <= 4) SELECT id FROM runnable ORDER BY id";
    assert_eq!(ids(&pool, query).await, vec![10, 20, 21]);

    // The correlation only in the ON, at the top level.
    assert_eq!(
        ids(
            &pool,
            "SELECT id FROM vynchat_swarm_task task WHERE NOT EXISTS (SELECT 1 FROM vynchat_swarm s \
             JOIN vynchat_swarm_task prior ON prior.swarm_id = s.id AND prior.task_index < task.task_index \
             WHERE s.sequential) ORDER BY id"
        )
        .await,
        vec![10, 20]
    );
    // A window beside a correlated EXISTS comparing by `<`.
    let rows = sqlx::query(
        "SELECT id, ROW_NUMBER() OVER (ORDER BY id) AS rk FROM vynchat_swarm_task task WHERE NOT EXISTS \
         (SELECT 1 FROM vynchat_swarm_task prior WHERE prior.swarm_id = task.swarm_id \
          AND prior.task_index < task.task_index) ORDER BY id",
    )
    .fetch_all(&pool)
    .await
    .expect("window with a correlated EXISTS");
    let got: Vec<(i32, i64)> = rows.iter().map(|r| (r.get(0), r.get(1))).collect();
    assert_eq!(got, vec![(10, 1), (20, 2)]);
    // A CTE reading a derived table is described with its columns: the row
    // description had none, and sqlx failed on every row.
    assert_eq!(
        ids(
            &pool,
            "WITH r AS (SELECT id FROM (SELECT id, swarm_id FROM vynchat_swarm_task) x WHERE x.swarm_id = 2) \
             SELECT id FROM r ORDER BY id"
        )
        .await,
        vec![20, 21]
    );
    assert_eq!(
        ids(&pool, "WITH r AS (SELECT id FROM vynchat_swarm_task) SELECT id FROM (SELECT id FROM r) y ORDER BY id").await,
        vec![10, 11, 20, 21]
    );
    // An alias written without AS names the derived table's column.
    assert_eq!(
        ids(
            &pool,
            "SELECT id FROM (SELECT id, ROW_NUMBER() OVER (ORDER BY id) rn FROM vynchat_swarm_task) r \
             WHERE r.rn <= 2 ORDER BY id"
        )
        .await,
        vec![10, 11]
    );
}

/// `make_interval` over a column, as pms's escalation query writes it.
#[tokio::test(flavor = "multi_thread")]
async fn make_interval_takes_a_column() {
    let (_s, pool) = server().await;
    run(&pool, "CREATE TABLE pol (id int PRIMARY KEY, h int)").await;
    run(&pool, "INSERT INTO pol VALUES (1, 3), (2, NULL)").await;
    let rows = sqlx::query(
        "SELECT id, (make_interval(hours => p.h) = INTERVAL '3 hours') AS three FROM pol p ORDER BY id",
    )
    .fetch_all(&pool)
    .await
    .expect("make_interval over a column");
    let got: Vec<(i32, Option<bool>)> = rows.iter().map(|r| (r.get(0), r.get(1))).collect();
    // NULL in, NULL out, as in PostgreSQL.
    assert_eq!(got, vec![(1, Some(true)), (2, None)]);
}

/// A default written as a parenthesised cast, as ORMs and pg_dump write it.
#[tokio::test(flavor = "multi_thread")]
async fn a_parenthesised_cast_default_is_taken() {
    let (_s, pool) = server().await;
    run(
        &pool,
        "CREATE TABLE d6 (id int PRIMARY KEY, j jsonb DEFAULT ('[]'::jsonb), o jsonb DEFAULT ('{}'::jsonb), \
         a text[] DEFAULT ('{}'::text[]))",
    )
    .await;
    run(&pool, "INSERT INTO d6 (id) VALUES (1)").await;
    let row = sqlx::query("SELECT j::text, o::text, a::text FROM d6").fetch_one(&pool).await.unwrap();
    assert_eq!(
        (row.get::<String, _>(0), row.get::<String, _>(1), row.get::<String, _>(2)),
        ("[]".to_string(), "{}".to_string(), "{}".to_string())
    );
}

/// `SHOW TABLES` names every column of a composite primary key.
#[tokio::test(flavor = "multi_thread")]
async fn show_tables_names_a_composite_primary_key() {
    let (_s, pool) = server().await;
    run(&pool, "CREATE TABLE zz_pk (id uuid, d uuid, PRIMARY KEY (id, d))").await;
    let rows = sqlx::raw_sql("SHOW TABLES").fetch_all(&pool).await.expect("SHOW TABLES");
    let key = rows
        .iter()
        .find(|r| r.get::<String, _>("table_name") == "zz_pk")
        .map(|r| r.get::<String, _>("primary_key"))
        .expect("zz_pk listed");
    assert_eq!(key, "id, d");
}
