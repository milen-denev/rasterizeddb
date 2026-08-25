//! Foreign key enforcement.
//!
//! A foreign key is the one constraint an application genuinely delegates to the
//! database, because the alternative is every writer remembering to check.
//! Accepting the DDL and enforcing nothing would leave an application believing
//! its references are sound when they are not, so these tests assert that a
//! violation is *refused* rather than recorded.

use sqlx::AssertSqlSafe;
use rasterizeddb_compat::{TestServer, probes};

async fn server() -> (TestServer, sqlx::PgPool) {
    let server = TestServer::start().await.expect("server should start");
    let pool = server.connect_sqlx().await.expect("should connect");
    probes::apply_schema(&pool).await.expect("schema should apply");
    (server, pool)
}

async fn run(pool: &sqlx::PgPool, sql: &str) -> Result<(), String> {
    sqlx::query(AssertSqlSafe(sql))
        .execute(pool)
        .await
        .map(|_| ())
        .map_err(|e| e.to_string().lines().next().unwrap_or("").to_string())
}

async fn count(pool: &sqlx::PgPool, sql: &str) -> usize {
    sqlx::query(AssertSqlSafe(sql)).fetch_all(pool).await.expect("select should succeed").len()
}

/// Builds a parent and a child related by `action`.
async fn with_action(pool: &sqlx::PgPool, suffix: &str, action: &str) {
    run(pool, &format!("CREATE TABLE p{} (id INT NOT NULL, name TEXT NOT NULL)", suffix))
        .await
        .expect("parent");
    run(
        pool,
        &format!(
            "CREATE TABLE c{s} (id INT NOT NULL, pid INT, \
             CONSTRAINT fk{s} FOREIGN KEY (pid) REFERENCES p{s} (id) ON DELETE {a})",
            s = suffix,
            a = action
        ),
    )
    .await
    .expect("child");
    run(pool, &format!("INSERT INTO p{} (id, name) VALUES (1, 'a')", suffix))
        .await
        .expect("parent row");
    run(pool, &format!("INSERT INTO c{} (id, pid) VALUES (10, 1)", suffix))
        .await
        .expect("child row");
}

#[tokio::test(flavor = "multi_thread")]
async fn an_insert_referencing_a_missing_row_is_refused() {
    let (_s, pool) = server().await;
    with_action(&pool, "_ins", "CASCADE").await;

    let err = run(&pool, "INSERT INTO c_ins (id, pid) VALUES (11, 999)")
        .await
        .expect_err("999 is not a parent");
    assert!(err.contains("foreign key constraint"), "{}", err);
    assert!(err.contains("fk_ins"), "the message should name the constraint: {}", err);

    assert_eq!(count(&pool, "SELECT id FROM c_ins").await, 1, "nothing was written");
}

/// A foreign key constrains what a value may be, not whether there is one.
#[tokio::test(flavor = "multi_thread")]
async fn a_null_reference_is_allowed() {
    let (_s, pool) = server().await;
    with_action(&pool, "_null", "CASCADE").await;

    run(&pool, "INSERT INTO c_null (id, pid) VALUES (11, NULL)")
        .await
        .expect("NULL is exempt");
    assert_eq!(count(&pool, "SELECT id FROM c_null").await, 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn an_update_to_a_missing_row_is_refused() {
    let (_s, pool) = server().await;
    with_action(&pool, "_upd", "CASCADE").await;

    let err = run(&pool, "UPDATE c_upd SET pid = 999 WHERE id = 10")
        .await
        .expect_err("999 is not a parent");
    assert!(err.contains("foreign key constraint"), "{}", err);

    // The original reference survived the refusal.
    assert_eq!(count(&pool, "SELECT id FROM c_upd WHERE pid = 1").await, 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn on_delete_cascade_removes_referencing_rows() {
    let (_s, pool) = server().await;
    with_action(&pool, "_cas", "CASCADE").await;

    run(&pool, "DELETE FROM p_cas WHERE id = 1").await.expect("delete");
    assert_eq!(count(&pool, "SELECT id FROM c_cas").await, 0, "the child went with the parent");
}

#[tokio::test(flavor = "multi_thread")]
async fn on_delete_restrict_refuses_while_a_reference_remains() {
    let (_s, pool) = server().await;
    with_action(&pool, "_res", "RESTRICT").await;

    let err = run(&pool, "DELETE FROM p_res WHERE id = 1")
        .await
        .expect_err("a child still references it");
    assert!(err.contains("foreign key constraint"), "{}", err);

    assert_eq!(count(&pool, "SELECT id FROM p_res").await, 1, "the parent survived");
    assert_eq!(count(&pool, "SELECT id FROM c_res").await, 1, "so did the child");
}

#[tokio::test(flavor = "multi_thread")]
async fn on_delete_set_null_clears_the_reference() {
    let (_s, pool) = server().await;
    with_action(&pool, "_sn", "SET NULL").await;

    run(&pool, "DELETE FROM p_sn WHERE id = 1").await.expect("delete");
    assert_eq!(count(&pool, "SELECT id FROM c_sn").await, 1, "the child stayed");
    assert_eq!(
        count(&pool, "SELECT id FROM c_sn WHERE pid IS NULL").await,
        1,
        "its reference was cleared rather than left dangling"
    );
}

/// `SET DEFAULT` puts the reference back to the column's declared default,
/// which is only meaningful now that defaults exist.
#[tokio::test(flavor = "multi_thread")]
async fn on_delete_set_default_restores_the_declared_default() {
    let (_s, pool) = server().await;

    run(&pool, "CREATE TABLE p_sd (id INT NOT NULL)").await.expect("parent");
    run(
        &pool,
        "CREATE TABLE c_sd (id INT NOT NULL, pid INT DEFAULT 9, \
         FOREIGN KEY (pid) REFERENCES p_sd (id) ON DELETE SET DEFAULT)",
    )
    .await
    .expect("child");
    // 9 has to exist, or restoring the default would violate the very key
    // being maintained.
    run(&pool, "INSERT INTO p_sd (id) VALUES (1)").await.expect("parent row");
    run(&pool, "INSERT INTO p_sd (id) VALUES (9)").await.expect("default row");
    run(&pool, "INSERT INTO c_sd (id, pid) VALUES (1, 1)").await.expect("child row");

    run(&pool, "DELETE FROM p_sd WHERE id = 1").await.expect("delete");
    assert_eq!(count(&pool, "SELECT id FROM c_sd").await, 1, "the child stayed");
    assert_eq!(
        count(&pool, "SELECT id FROM c_sd WHERE pid = 9").await,
        1,
        "its reference went back to the default rather than being cleared"
    );
}

/// With no default declared the column reads as `NULL`, as it does in
/// PostgreSQL.
#[tokio::test(flavor = "multi_thread")]
async fn set_default_without_a_default_clears_the_reference() {
    let (_s, pool) = server().await;
    with_action(&pool, "_sd2", "SET DEFAULT").await;

    run(&pool, "DELETE FROM p_sd2 WHERE id = 1").await.expect("delete");
    assert_eq!(
        count(&pool, "SELECT id FROM c_sd2 WHERE pid IS NULL").await,
        1,
        "no declared default means NULL"
    );
}

/// A cascade that triggers another cascade works by recursion, not by a second
/// mechanism, so a whole chain goes at once.
#[tokio::test(flavor = "multi_thread")]
async fn a_cascade_propagates_through_a_chain() {
    let (_s, pool) = server().await;

    run(&pool, "CREATE TABLE a1 (id INT NOT NULL)").await.expect("a1");
    run(
        &pool,
        "CREATE TABLE b1 (id INT NOT NULL, aid INT, \
         CONSTRAINT fk_b1 FOREIGN KEY (aid) REFERENCES a1 (id) ON DELETE CASCADE)",
    )
    .await
    .expect("b1");
    run(
        &pool,
        "CREATE TABLE c1 (id INT NOT NULL, bid INT, \
         CONSTRAINT fk_c1 FOREIGN KEY (bid) REFERENCES b1 (id) ON DELETE CASCADE)",
    )
    .await
    .expect("c1");

    run(&pool, "INSERT INTO a1 (id) VALUES (1)").await.expect("a row");
    run(&pool, "INSERT INTO b1 (id, aid) VALUES (2, 1)").await.expect("b row");
    run(&pool, "INSERT INTO c1 (id, bid) VALUES (3, 2)").await.expect("c row");

    run(&pool, "DELETE FROM a1 WHERE id = 1").await.expect("delete");
    assert_eq!(count(&pool, "SELECT id FROM b1").await, 0);
    assert_eq!(count(&pool, "SELECT id FROM c1").await, 0, "the cascade reached the grandchild");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_constraint_added_later_is_enforced_and_can_be_dropped() {
    let (_s, pool) = server().await;

    run(&pool, "CREATE TABLE p_alt (id INT NOT NULL)").await.expect("parent");
    run(&pool, "CREATE TABLE c_alt (id INT NOT NULL, pid INT)").await.expect("child");
    run(&pool, "INSERT INTO p_alt (id) VALUES (1)").await.expect("parent row");

    // Unconstrained, so anything goes.
    run(&pool, "INSERT INTO c_alt (id, pid) VALUES (10, 999)").await.expect("before");

    run(
        &pool,
        "ALTER TABLE c_alt ADD CONSTRAINT fk_alt FOREIGN KEY (pid) REFERENCES p_alt (id)",
    )
    .await
    .expect("add constraint");

    let err = run(&pool, "INSERT INTO c_alt (id, pid) VALUES (11, 998)")
        .await
        .expect_err("now constrained");
    assert!(err.contains("fk_alt"), "{}", err);

    run(&pool, "ALTER TABLE c_alt DROP CONSTRAINT fk_alt").await.expect("drop constraint");
    run(&pool, "INSERT INTO c_alt (id, pid) VALUES (12, 997)")
        .await
        .expect("unconstrained again");
}

/// Composite keys are refused outright: enforcing one column of a two-column key
/// would enforce something weaker than what was declared.
#[tokio::test(flavor = "multi_thread")]
async fn a_composite_key_is_refused_rather_than_partly_enforced() {
    let (_s, pool) = server().await;

    run(&pool, "CREATE TABLE p_comp (a INT NOT NULL, b INT NOT NULL)").await.expect("parent");
    let err = run(
        &pool,
        "CREATE TABLE c_comp (a INT NOT NULL, b INT NOT NULL, \
         FOREIGN KEY (a, b) REFERENCES p_comp (a, b))",
    )
    .await
    .expect_err("composite");
    assert!(err.contains("composite"), "{}", err);
}
