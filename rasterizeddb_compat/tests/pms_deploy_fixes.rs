//! The statements pms ran while migrating onto a 1.0.0 server, each answered
//! as PostgreSQL 17 answers it (1.0.2).
//!
//! Every one failed before: a transaction dropping a child then its parent, a
//! correlated reference in an inner join's `ON` inside `EXISTS`, a window
//! beside such a subquery, `make_interval` over a column, a parenthesised
//! cast as a `DEFAULT`, an alias written without `AS`, `SHOW TABLES`
//! naming a composite key, `GROUP BY 1` over a join, and a statement with
//! more than 32,767 bound parameters.

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

/// pms's claim statement as it really runs: CTEs over a windowed derived table
/// ending in `UPDATE … RETURNING`, bound over the extended protocol, with rows
/// to decode.
const CLAIM: &str = "WITH blocked AS (SELECT task.id FROM vynchat_swarm_task task \
       JOIN vynchat_swarm s ON s.id = task.swarm_id WHERE s.sequential AND EXISTS \
       (SELECT 1 FROM vynchat_swarm_task prior WHERE prior.swarm_id = task.swarm_id \
        AND prior.task_index < task.task_index AND prior.status = 'pending')), \
     runnable AS (SELECT id FROM (SELECT id, ROW_NUMBER() OVER (PARTITION BY domain_id \
        ORDER BY created_at, task_index) AS domain_rank FROM vynchat_swarm_task \
        WHERE status = 'pending' AND id NOT IN (SELECT id FROM blocked)) ranked \
        WHERE ranked.domain_rank <= $1), \
     claimed AS (SELECT t.id FROM vynchat_swarm_task t WHERE t.id IN (SELECT id FROM runnable) \
        ORDER BY t.created_at LIMIT $2 FOR UPDATE SKIP LOCKED) \
     UPDATE vynchat_swarm_task AS t SET status = 'running' FROM claimed c WHERE t.id = c.id \
     RETURNING t.id, t.swarm_id, t.status";

#[tokio::test(flavor = "multi_thread")]
async fn the_claim_update_returning_is_described_and_decoded() {
    let (_s, pool) = server().await;
    swarm(&pool).await;
    let rows = sqlx::query(CLAIM)
        .bind(4i64)
        .bind(8i64)
        .fetch_all(&pool)
        .await
        .expect("the claim statement");
    let mut got: Vec<(i32, i32, String)> = rows
        .iter()
        .map(|r| (r.try_get(0).unwrap(), r.try_get(1).unwrap(), r.try_get(2).unwrap()))
        .collect();
    got.sort();
    let running = |id, swarm| (id, swarm, "running".to_string());
    assert_eq!(got, vec![running(10, 1), running(20, 2), running(21, 2)]);
}

/// SQL-level `PREPARE` / `EXECUTE` with a parameter in a CTE's `LIMIT`.
#[tokio::test(flavor = "multi_thread")]
async fn a_prepared_limit_inside_a_cte_takes_its_parameter() {
    let (_s, pool) = server().await;
    swarm(&pool).await;
    let mut conn = pool.acquire().await.unwrap();
    sqlx::raw_sql(
        "PREPARE firsts (bigint) AS WITH c AS (SELECT id FROM vynchat_swarm_task ORDER BY id LIMIT $1) \
         SELECT id FROM c ORDER BY id",
    )
    .execute(&mut *conn)
    .await
    .expect("PREPARE");
    let rows = sqlx::raw_sql("EXECUTE firsts (2)").fetch_all(&mut *conn).await.expect("EXECUTE");
    let got: Vec<i32> = rows.iter().map(|r| r.get(0)).collect();
    assert_eq!(got, vec![10, 11]);
    // The claim statement itself, as pms prepared it by hand.
    sqlx::raw_sql(AssertSqlSafe(format!("PREPARE claim (bigint, bigint) AS {CLAIM}")))
        .execute(&mut *conn)
        .await
        .expect("PREPARE claim");
    let rows = sqlx::raw_sql("EXECUTE claim (4, 8)").fetch_all(&mut *conn).await.expect("EXECUTE claim");
    assert_eq!(rows.len(), 3);
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

/// `GROUP BY 1` over a join was looked up as a column named `1` ("Column '1'
/// not found in schema"); on one table it already named the first item.
#[tokio::test]
async fn group_by_a_position_or_an_output_name_over_a_join() {
    let (_s, pool) = server().await;
    run(&pool, "CREATE TABLE zz_ut (id int PRIMARY KEY, system_key text)").await;
    run(&pool, "CREATE TABLE zz_utr (id int PRIMARY KEY, table_id int)").await;
    run(&pool, "INSERT INTO zz_ut VALUES (1, 'a'), (2, 'b')").await;
    run(&pool, "INSERT INTO zz_utr VALUES (1, 1), (2, 1), (3, 2)").await;
    let counts = |sql: &'static str| {
        let pool = pool.clone();
        async move {
            sqlx::query(AssertSqlSafe(sql))
                .fetch_all(&pool)
                .await
                .unwrap_or_else(|e| panic!("`{sql}` failed: {e}"))
                .iter()
                .map(|r| (r.get::<String, _>(0), r.get::<i64, _>(1)))
                .collect::<Vec<_>>()
        }
    };
    let want = vec![("a".to_string(), 2), ("b".to_string(), 1)];
    for sql in [
        "SELECT t.system_key, count(*) FROM zz_utr r JOIN zz_ut t ON t.id = r.table_id GROUP BY 1 ORDER BY 1",
        "SELECT t.system_key AS k, count(*) FROM zz_utr r JOIN zz_ut t ON t.id = r.table_id GROUP BY 1 ORDER BY 1",
        "SELECT t.system_key k, count(*) FROM zz_utr r JOIN zz_ut t ON t.id = r.table_id GROUP BY k ORDER BY k",
        "SELECT t.system_key, count(*) FROM zz_utr r JOIN zz_ut t ON t.id = r.table_id GROUP BY t.system_key ORDER BY 1",
    ] {
        assert_eq!(counts(sql).await, want, "{sql}");
    }
    let e = sqlx::query("SELECT t.system_key, count(*) FROM zz_utr r JOIN zz_ut t ON t.id = r.table_id GROUP BY 3")
        .fetch_all(&pool)
        .await
        .expect_err("a position past the select list");
    let db = e.as_database_error().expect("a server error");
    assert_eq!(db.code().as_deref(), Some("42P10"), "{e}");
}

/// A Bind's parameter count is an Int16 that PostgreSQL reads unsigned, up to
/// 65,535. Read signed, pms's 39,000-parameter companion insert was -26,536
/// parameters: the server ran out of message and dropped the connection with
/// nothing in its log, and the client read "expected to read 5 bytes, got 0
/// bytes at EOF".
#[tokio::test]
async fn a_statement_with_more_than_32767_parameters_is_bound() {
    let (_s, pool) = server().await;
    run(&pool, "CREATE TABLE zz_wide (id int PRIMARY KEY, v int)").await;
    let rows = 20_000; // 40,000 parameters
    let values: Vec<String> = (0..rows).map(|r| format!("(${}, ${})", 2 * r + 1, 2 * r + 2)).collect();
    let sql = format!("INSERT INTO zz_wide (id, v) VALUES {} ON CONFLICT (id) DO NOTHING", values.join(", "));
    let mut query = sqlx::query(AssertSqlSafe(sql.as_str()));
    for r in 0..rows {
        query = query.bind(r).bind(r * 3);
    }
    let done = query.execute(&pool).await.unwrap_or_else(|e| panic!("40,000 parameters: {e}"));
    assert_eq!(done.rows_affected(), rows as u64);
    let (count, sum): (i64, i64) =
        sqlx::query_as("SELECT count(*), sum(v)::bigint FROM zz_wide").fetch_one(&pool).await.unwrap();
    assert_eq!(count, rows as i64);
    assert_eq!(sum, (0..rows as i64).map(|r| r * 3).sum::<i64>());
}
