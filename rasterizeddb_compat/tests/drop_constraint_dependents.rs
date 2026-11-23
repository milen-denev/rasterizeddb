//! A key a foreign key references is not dropped out from under it.
//!
//! `ALTER TABLE t1 DROP CONSTRAINT t1_pkey` succeeded while `ref1.t1_id`
//! still referenced `t1(id)`. The foreign key stayed, enforced against a
//! column nothing kept unique any more. PostgreSQL refuses the drop
//! (2BP01, "cannot drop constraint t1_pkey on table t1 because other
//! objects depend on it") unless it says `CASCADE`, which drops the
//! dependent foreign keys with it. The same for a UNIQUE column, a named
//! UNIQUE constraint and a table's reference to itself.
//!
//! Every expectation is PostgreSQL 17's answer to the same statements.

use rasterizeddb_compat::TestServer;
use sqlx::{AssertSqlSafe, Executor, Row};

const PARENT: &str = "aaaaaaaa-0000-4000-8000-000000000001";

async fn start() -> (TestServer, sqlx::PgPool) {
    let server = TestServer::start().await.expect("server");
    let pool = server.connect_sqlx().await.expect("connect");
    pool.execute(sqlx::raw_sql(AssertSqlSafe(format!(
        r#"CREATE TABLE "t1" ("id" uuid NOT NULL PRIMARY KEY, n int UNIQUE, m int, CONSTRAINT t1_m_key UNIQUE (m));
           CREATE TABLE "ref1" ("id" int PRIMARY KEY, "t1_id" uuid,
                                CONSTRAINT "fk_ref1_t1" FOREIGN KEY ("t1_id") REFERENCES "t1" ("id"));
           CREATE TABLE ref2 (id int PRIMARY KEY, n int REFERENCES t1 (n), m int REFERENCES t1 (m));
           CREATE TABLE ref3 (id int PRIMARY KEY, n int REFERENCES t1 (n));
           INSERT INTO t1 VALUES ('{PARENT}', 1, 1);
           INSERT INTO ref1 VALUES (1, '{PARENT}');"#
    ))))
    .await
    .expect("setup");
    (server, pool)
}

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

async fn run(pool: &sqlx::PgPool, sql: &str) {
    pool.execute(sqlx::raw_sql(AssertSqlSafe(sql.to_string()))).await.unwrap_or_else(|e| panic!("`{sql}` should succeed: {e}"));
}

/// The SQLSTATE and message `sql` fails with.
async fn refused(pool: &sqlx::PgPool, sql: &str) -> (String, String) {
    let e = sqlx::raw_sql(AssertSqlSafe(sql.to_string())).fetch_all(pool).await.expect_err(sql);
    let db = e.as_database_error().unwrap_or_else(|| panic!("{sql}: not a server error: {e}"));
    (db.code().map(|c| c.to_string()).unwrap_or_default(), db.message().to_string())
}

/// Every constraint of the test's tables, as `name|type`.
async fn constraints(pool: &sqlx::PgPool) -> Vec<String> {
    rows(
        pool,
        "SELECT conname, contype FROM pg_constraint \
         WHERE conrelid IN ('\"t1\"'::regclass, 'ref1'::regclass, 'ref2'::regclass, 'ref3'::regclass) ORDER BY conname",
    )
    .await
}

#[tokio::test(flavor = "multi_thread")]
async fn a_referenced_key_is_not_dropped_without_cascade() {
    let (_server, pool) = start().await;
    let before = constraints(&pool).await;
    for (sql, constraint) in [
        // The statement the migration sent: an inline primary key.
        (r#"ALTER TABLE "t1" DROP CONSTRAINT t1_pkey"#, "t1_pkey"),
        (r#"ALTER TABLE "t1" DROP CONSTRAINT t1_pkey RESTRICT"#, "t1_pkey"),
        (r#"ALTER TABLE "t1" DROP CONSTRAINT "t1_pkey";"#, "t1_pkey"),
        // IF EXISTS is about the constraint existing, not about dependents.
        ("ALTER TABLE t1 DROP CONSTRAINT IF EXISTS t1_pkey", "t1_pkey"),
        // A UNIQUE column, and a named UNIQUE constraint.
        ("ALTER TABLE t1 DROP CONSTRAINT t1_n_key", "t1_n_key"),
        ("ALTER TABLE t1 DROP CONSTRAINT t1_m_key", "t1_m_key"),
    ] {
        let (code, message) = refused(&pool, sql).await;
        assert_eq!(code, "2BP01", "{sql}: {message}");
        assert!(
            message.starts_with(&format!("cannot drop constraint {constraint} on table t1 because other objects depend on it")),
            "{sql}: {message}"
        );
    }
    // Nothing went: the keys still hold, on both sides.
    assert_eq!(constraints(&pool).await, before);
    assert_eq!(
        before,
        ["fk_ref1_t1|f", "ref1_pkey|p", "ref2_m_fkey|f", "ref2_n_fkey|f", "ref2_pkey|p", "ref3_n_fkey|f", "ref3_pkey|p", "t1_m_key|u", "t1_n_key|u", "t1_pkey|p"]
    );
    assert_eq!(refused(&pool, "INSERT INTO ref1 VALUES (2, 'bbbbbbbb-0000-4000-8000-000000000002')").await.0, "23503");
    assert_eq!(refused(&pool, &format!("INSERT INTO t1 VALUES ('{PARENT}', 2, 2)")).await.0, "23505");
    assert_eq!(refused(&pool, "INSERT INTO t1 VALUES ('aaaaaaaa-0000-4000-8000-000000000002', 1, 2)").await.0, "23505");
    // A name that is no constraint is 42704, CASCADE or not; IF EXISTS skips it.
    assert_eq!(refused(&pool, "ALTER TABLE t1 DROP CONSTRAINT nosuch").await.0, "42704");
    assert_eq!(refused(&pool, "ALTER TABLE t1 DROP CONSTRAINT nosuch CASCADE").await.0, "42704");
    run(&pool, "ALTER TABLE t1 DROP CONSTRAINT IF EXISTS nosuch CASCADE").await;
    assert_eq!(constraints(&pool).await, before);
}

#[tokio::test(flavor = "multi_thread")]
async fn cascade_drops_the_foreign_keys_that_depend_on_the_key() {
    let (_server, pool) = start().await;
    // A UNIQUE column two tables reference: both keys go, and only they.
    run(&pool, "ALTER TABLE t1 DROP CONSTRAINT t1_n_key CASCADE").await;
    assert_eq!(
        constraints(&pool).await,
        ["fk_ref1_t1|f", "ref1_pkey|p", "ref2_m_fkey|f", "ref2_pkey|p", "ref3_pkey|p", "t1_m_key|u", "t1_pkey|p"]
    );
    run(&pool, "INSERT INTO ref2 VALUES (2, 5, NULL)").await;
    run(&pool, "INSERT INTO ref3 VALUES (2, 5)").await;
    assert_eq!(refused(&pool, "INSERT INTO ref2 VALUES (3, NULL, 5)").await.0, "23503");
    // The column is no longer unique.
    run(&pool, "INSERT INTO t1 VALUES ('aaaaaaaa-0000-4000-8000-000000000002', 1, 2)").await;

    // The primary key, with IF EXISTS and a trailing semicolon.
    run(&pool, r#"ALTER TABLE "t1" DROP CONSTRAINT IF EXISTS t1_pkey CASCADE;"#).await;
    assert_eq!(constraints(&pool).await, ["ref1_pkey|p", "ref2_m_fkey|f", "ref2_pkey|p", "ref3_pkey|p", "t1_m_key|u"]);
    run(&pool, "INSERT INTO ref1 VALUES (3, 'cccccccc-0000-4000-8000-000000000003')").await;
    run(&pool, "INSERT INTO t1 VALUES ('aaaaaaaa-0000-4000-8000-000000000002', 7, 7)").await;
    // A parent row leaves freely: nothing references it by a key any more.
    run(&pool, &format!("DELETE FROM t1 WHERE id = '{PARENT}'")).await;

    // A named UNIQUE constraint, the keyword in lower case.
    run(&pool, "ALTER TABLE t1 DROP CONSTRAINT t1_m_key cascade").await;
    assert_eq!(constraints(&pool).await, ["ref1_pkey|p", "ref2_pkey|p", "ref3_pkey|p"]);
    run(&pool, "INSERT INTO ref2 VALUES (4, NULL, 9)").await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_table_level_key_and_a_self_reference_follow_the_same_rule() {
    let server = TestServer::start().await.expect("server");
    let pool = server.connect_sqlx().await.expect("connect");
    run(
        &pool,
        r#"CREATE TABLE tl (id int, CONSTRAINT tl_pkey PRIMARY KEY (id));
           CREATE TABLE tlr (id int PRIMARY KEY, p int REFERENCES tl);
           CREATE TABLE s (id int PRIMARY KEY, p int REFERENCES s (id));
           CREATE TABLE "Mixed" ("Id" int PRIMARY KEY);
           CREATE TABLE mixr (id int PRIMARY KEY, p int, CONSTRAINT "Mix FK" FOREIGN KEY (p) REFERENCES "Mixed" ("Id"));"#,
    )
    .await;
    for sql in [
        "ALTER TABLE tl DROP CONSTRAINT tl_pkey",
        "ALTER TABLE s DROP CONSTRAINT s_pkey",
        r#"ALTER TABLE "Mixed" DROP CONSTRAINT "Mixed_pkey""#,
    ] {
        assert_eq!(refused(&pool, sql).await.0, "2BP01", "{sql}");
    }
    let of = |tables: &str| format!("SELECT conname, contype FROM pg_constraint WHERE conrelid IN ({tables}) ORDER BY conname");

    run(&pool, "ALTER TABLE tl DROP CONSTRAINT tl_pkey CASCADE").await;
    assert_eq!(rows(&pool, &of("'tl'::regclass, 'tlr'::regclass")).await, ["tlr_pkey|p"]);
    run(&pool, "INSERT INTO tlr VALUES (1, 42)").await;

    // The table's own reference to itself goes with its key.
    run(&pool, "ALTER TABLE s DROP CONSTRAINT s_pkey CASCADE").await;
    assert_eq!(rows(&pool, &of("'s'::regclass")).await, Vec::<String>::new());
    run(&pool, "INSERT INTO s VALUES (1, 99), (1, 98)").await;

    run(&pool, r#"ALTER TABLE "Mixed" DROP CONSTRAINT "Mixed_pkey" CASCADE"#).await;
    assert_eq!(rows(&pool, &of("'\"Mixed\"'::regclass, 'mixr'::regclass")).await, ["mixr_pkey|p"]);
}

/// What does not depend on a key leaves it free to go, as before.
#[tokio::test(flavor = "multi_thread")]
async fn a_key_nothing_references_is_dropped_as_before() {
    let server = TestServer::start().await.expect("server");
    let pool = server.connect_sqlx().await.expect("connect");
    run(
        &pool,
        "CREATE TABLE u (id int PRIMARY KEY, a int UNIQUE);
         CREATE TABLE ur (id int PRIMARY KEY, a int REFERENCES u (a));
         CREATE TABLE k (a int, b int, PRIMARY KEY (a, b));",
    )
    .await;
    // The primary key: only `a` is referenced.
    run(&pool, "ALTER TABLE u DROP CONSTRAINT u_pkey").await;
    run(&pool, "ALTER TABLE u ADD PRIMARY KEY (id, a)").await;
    assert_eq!(refused(&pool, "ALTER TABLE u DROP CONSTRAINT u_a_key").await.0, "2BP01");
    // Once the foreign key is gone, so is the dependency.
    run(&pool, "ALTER TABLE ur DROP CONSTRAINT ur_a_fkey").await;
    run(&pool, "ALTER TABLE u DROP CONSTRAINT u_a_key").await;
    run(&pool, "ALTER TABLE k DROP CONSTRAINT k_pkey").await;
    run(&pool, "INSERT INTO k VALUES (1, 1), (1, 1)").await;
}
