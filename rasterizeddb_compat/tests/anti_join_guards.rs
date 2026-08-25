//! The role-seed guards: every INSERT into roles is protected only by an
//! existence check, so a guard that under-filters creates duplicates
//! silently — and once the unique constraint's migration re-runs, it fails
//! on data a correct guard would never have written.
//!
//! Shapes verbatim from the pms migrations (m20260124_000006 and the
//! Zapier/Google connector seeds).

use rasterizeddb_compat::TestServer;
use sqlx::{Executor, Row};

async fn server() -> (TestServer, sqlx::PgPool) {
    let server = TestServer::start().await.expect("server should start");
    let pool = server.connect_sqlx().await.expect("should connect");
    (server, pool)
}

async fn run(pool: &sqlx::PgPool, sql: &str) -> Result<(), String> {
    pool.execute(sqlx::raw_sql(sql))
        .await
        .map(|_| ())
        .map_err(|e| e.to_string().lines().next().unwrap_or("").to_string())
}

async fn count(pool: &sqlx::PgPool, sql: &str) -> i64 {
    pool.fetch_one(sqlx::raw_sql(sql))
        .await
        .expect("count query")
        .try_get::<i64, _>(0)
        .expect("count")
}

async fn seed_domains(pool: &sqlx::PgPool) {
    run(pool, "CREATE TABLE domain (id UUID PRIMARY KEY, name TEXT NOT NULL)")
        .await
        .expect("create domain");
    run(pool, "CREATE TABLE roles (id UUID PRIMARY KEY, domain_id UUID, name TEXT NOT NULL, date_added TIMESTAMPTZ)")
        .await
        .expect("create roles");
    for (id, name) in [
        ("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa", "d1"),
        ("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb", "d2"),
        ("cccccccc-cccc-cccc-cccc-cccccccccccc", "d3"),
    ] {
        run(pool, &format!("INSERT INTO domain (id, name) VALUES ('{}', '{}')", id, name))
            .await
            .expect("insert domain");
    }
    // d1 already has its SuperAdministrator; the guard must skip it.
    run(pool, "INSERT INTO roles (id, domain_id, name, date_added) VALUES ('11111111-1111-1111-1111-111111111111', 'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa', 'SuperAdministrator', now())")
        .await
        .expect("preexisting role");
}

const SEED: &str = "WITH missing_domains AS ( \
    SELECT d.id FROM domain d \
    LEFT JOIN roles r ON r.domain_id = d.id AND r.name = 'SuperAdministrator' \
    WHERE r.id IS NULL \
) \
INSERT INTO roles (id, name, domain_id, date_added) \
SELECT gen_random_uuid(), 'SuperAdministrator', id, now() FROM missing_domains";

/// The m20260124_000006 anti-join: only domains MISSING the role get one,
/// and a re-run of the same migration writes nothing.
#[tokio::test]
async fn the_anti_join_seed_only_fills_missing_domains_and_is_idempotent() {
    let (_s, pool) = server().await;
    seed_domains(&pool).await;

    run(&pool, SEED).await.expect("the seed must run");
    let total = count(&pool, "SELECT COUNT(*) FROM roles WHERE name = 'SuperAdministrator'").await;
    assert_eq!(total, 3, "one role per domain — d1's was NOT duplicated");
    let dupes = count(
        &pool,
        "SELECT COUNT(*) FROM (SELECT domain_id, name FROM roles GROUP BY domain_id, name HAVING COUNT(*) > 1) AS d",
    )
    .await;
    assert_eq!(dupes, 0, "no (domain_id, name) pair duplicated");

    // The production failure mode: the chain re-runs. The guard must now
    // filter EVERY domain out.
    run(&pool, SEED).await.expect("the seed must re-run");
    let total = count(&pool, "SELECT COUNT(*) FROM roles WHERE name = 'SuperAdministrator'").await;
    assert_eq!(total, 3, "a re-run writes nothing — this is what protects the unique migration");
}

/// The connector seeds: a data-modifying CTE whose NOT EXISTS guard reads a
/// sibling CTE. Run twice; the second run must insert nothing.
#[tokio::test]
async fn the_sibling_cte_guard_holds_across_reruns() {
    let (_s, pool) = server().await;
    seed_domains(&pool).await;

    let seed = "WITH domain_row AS ( \
        SELECT id FROM domain WHERE name = 'd2' \
    ), \
    role_row AS ( \
        SELECT id FROM roles WHERE domain_id = (SELECT id FROM domain_row) AND name = 'ZapierIntegration' \
    ) \
    INSERT INTO roles (id, name, domain_id, date_added) \
    SELECT gen_random_uuid(), 'ZapierIntegration', id, now() FROM domain_row \
    WHERE NOT EXISTS (SELECT 1 FROM role_row)";

    run(&pool, seed).await.expect("first run inserts");
    let total = count(&pool, "SELECT COUNT(*) FROM roles WHERE name = 'ZapierIntegration'").await;
    assert_eq!(total, 1, "one connector role");

    run(&pool, seed).await.expect("second run is guarded");
    let total = count(&pool, "SELECT COUNT(*) FROM roles WHERE name = 'ZapierIntegration'").await;
    assert_eq!(total, 1, "the guard saw the first run's row");
}
