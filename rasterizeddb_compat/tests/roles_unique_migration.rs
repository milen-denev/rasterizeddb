//! The `m20260124_000005_fix_role_name_uniques` shape: drop the old
//! constraints, add `UNIQUE (domain_id, name)` — over a roles table that
//! holds global roles with a NULL domain_id, which PostgreSQL's UNIQUE
//! treats as never-equal.

use rasterizeddb_compat::TestServer;
use sqlx::Executor;

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

#[tokio::test]
async fn the_roles_unique_migration_applies() {
    let (_s, pool) = server().await;
    run(&pool, "CREATE TABLE roles (id UUID PRIMARY KEY, domain_id UUID, name TEXT NOT NULL)")
        .await
        .expect("create");
    // Global roles: NULL domain_id, distinct names. Per-domain roles reuse
    // the same names — legal under (domain_id, name).
    for (id, domain, name) in [
        ("11111111-1111-1111-1111-111111111111", "NULL", "admin"),
        ("22222222-2222-2222-2222-222222222222", "NULL", "user"),
        ("33333333-3333-3333-3333-333333333333", "'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa'", "admin"),
        ("44444444-4444-4444-4444-444444444444", "'bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb'", "admin"),
    ] {
        run(&pool, &format!(
            "INSERT INTO roles (id, domain_id, name) VALUES ('{}', {}, '{}')", id, domain, name))
            .await
            .expect("insert");
    }

    for sql in [
        "ALTER TABLE roles DROP CONSTRAINT IF EXISTS fk_roles_domain",
        "ALTER TABLE roles DROP CONSTRAINT IF EXISTS role_name_key",
        "ALTER TABLE roles DROP CONSTRAINT IF EXISTS idx_roles_domain_name",
    ] {
        run(&pool, sql).await.expect(sql);
    }
    run(&pool, "ALTER TABLE roles ADD CONSTRAINT idx_roles_domain_name UNIQUE (domain_id, name)")
        .await
        .expect("the composite unique must apply over NULL domains");

    // The constraint holds afterwards: a real duplicate is refused, a second
    // NULL-domain role with a fresh name is not.
    let dup = run(&pool, "INSERT INTO roles (id, domain_id, name) VALUES ('55555555-5555-5555-5555-555555555555', 'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa', 'admin')").await;
    assert!(dup.is_err(), "a true duplicate must be refused");
    run(&pool, "INSERT INTO roles (id, domain_id, name) VALUES ('66666666-6666-6666-6666-666666666666', NULL, 'viewer')")
        .await
        .expect("a NULL-domain role with a fresh name is legal");
}

/// The same migration inside a transaction, as sea-orm runs it.
#[tokio::test]
async fn the_migration_applies_inside_a_transaction() {
    let (_s, pool) = server().await;
    run(&pool, "CREATE TABLE roles (id UUID PRIMARY KEY, domain_id UUID, name TEXT NOT NULL)")
        .await
        .expect("create");
    run(&pool, "INSERT INTO roles (id, domain_id, name) VALUES ('11111111-1111-1111-1111-111111111111', NULL, 'admin')")
        .await
        .expect("insert");
    run(&pool, "INSERT INTO roles (id, domain_id, name) VALUES ('22222222-2222-2222-2222-222222222222', NULL, 'user')")
        .await
        .expect("insert");

    let mut tx = pool.begin().await.expect("begin");
    for sql in [
        "ALTER TABLE roles DROP CONSTRAINT IF EXISTS fk_roles_domain",
        "ALTER TABLE roles DROP CONSTRAINT IF EXISTS role_name_key",
        "ALTER TABLE roles DROP CONSTRAINT IF EXISTS idx_roles_domain_name",
        "ALTER TABLE roles ADD CONSTRAINT idx_roles_domain_name UNIQUE (domain_id, name)",
    ] {
        tx.execute(sqlx::raw_sql(sql)).await.unwrap_or_else(|e| panic!("{}: {}", sql, e));
    }
    tx.commit().await.expect("commit");
}

/// With a genuine duplicate pair the refusal must NAME the conflict — a bare
/// "Statement failed" leaves an operator with nothing to clean up by.
#[tokio::test]
async fn a_true_duplicate_is_refused_with_a_named_conflict() {
    let (_s, pool) = server().await;
    run(&pool, "CREATE TABLE roles (id UUID PRIMARY KEY, domain_id UUID, name TEXT NOT NULL)")
        .await
        .expect("create");
    run(&pool, "INSERT INTO roles (id, domain_id, name) VALUES ('11111111-1111-1111-1111-111111111111', 'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa', 'admin')")
        .await
        .expect("insert");
    run(&pool, "INSERT INTO roles (id, domain_id, name) VALUES ('22222222-2222-2222-2222-222222222222', 'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa', 'admin')")
        .await
        .expect("insert");

    let err = run(&pool, "ALTER TABLE roles ADD CONSTRAINT idx_roles_domain_name UNIQUE (domain_id, name)")
        .await
        .expect_err("duplicate data must refuse the constraint");
    assert!(
        err.to_lowercase().contains("admin"),
        "the refusal must NAME the conflicting values; got: {}",
        err
    );
    println!("refusal text: {}", err);
}
