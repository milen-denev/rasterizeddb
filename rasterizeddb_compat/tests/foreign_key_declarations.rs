//! What a table declaration may say, as PostgreSQL 17 decides it.
//!
//! Each of these was accepted, and each acceptance left a table that could
//! never work as written: a second column of one name that no statement could
//! reach, a foreign key whose every insert failed naming a missing table, and
//! a foreign key onto a non-unique column, under which deleting one of two
//! parent rows sharing a value acted on children the other still covered.

use rasterizeddb_compat::TestServer;
use sqlx::AssertSqlSafe;

async fn server() -> (TestServer, sqlx::PgPool) {
    let server = TestServer::start().await.expect("server should start");
    let pool = server.connect_sqlx().await.expect("should connect");
    (server, pool)
}

async fn run(pool: &sqlx::PgPool, sql: &str) -> Result<(), (String, String)> {
    sqlx::query(AssertSqlSafe(sql)).execute(pool).await.map(|_| ()).map_err(|e| {
        let code = e.as_database_error().and_then(|d| d.code()).map(|c| c.to_string()).unwrap_or_default();
        (code, e.to_string())
    })
}

async fn ok(pool: &sqlx::PgPool, sql: &str) {
    if let Err((code, e)) = run(pool, sql).await {
        panic!("`{sql}` must succeed, failed {code}: {e}");
    }
}

async fn refused(pool: &sqlx::PgPool, sql: &str, code: &str, words: &str) {
    match run(pool, sql).await {
        Ok(()) => panic!("`{sql}` was accepted; PostgreSQL refuses it with {code}"),
        Err((got, e)) => {
            assert_eq!(got, code, "`{sql}`: {e}");
            assert!(e.contains(words), "`{sql}`: {e}");
        }
    }
}

async fn exists(pool: &sqlx::PgPool, table: &str) -> bool {
    run(pool, &format!("SELECT * FROM {table}")).await.is_ok()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_column_named_twice_is_refused() {
    let (_s, pool) = server().await;
    refused(&pool, "CREATE TABLE dup1 (id INT, id INT)", "42701", "column \"id\" specified more than once").await;
    // Unquoted names fold, so `ID` is `id` again.
    refused(&pool, "CREATE TABLE dup2 (id INT, name TEXT, ID BIGINT)", "42701", "specified more than once").await;
    refused(&pool, "CREATE TABLE dup3 AS SELECT 1 AS a, 2 AS a", "42701", "specified more than once").await;
    for t in ["dup1", "dup2", "dup3"] {
        assert!(!exists(&pool, t).await, "{t} must not have been created");
    }
    // A quoted capital is another name, as in PostgreSQL.
    ok(&pool, "CREATE TABLE dup4 (\"A\" INT, a INT)").await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_foreign_key_to_a_missing_table_is_refused() {
    let (_s, pool) = server().await;
    for (name, sql) in [
        ("fm1", "CREATE TABLE fm1 (id INT REFERENCES nosuch (id))"),
        ("fm2", "CREATE TABLE fm2 (id INT, CONSTRAINT k FOREIGN KEY (id) REFERENCES nosuch (id))"),
        ("fm3", "CREATE TABLE fm3 (id INT REFERENCES nosuch)"),
    ] {
        refused(&pool, sql, "42P01", "relation \"nosuch\" does not exist").await;
        assert!(!exists(&pool, name).await, "{name} must not have been created");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn references_without_a_column_names_the_primary_key() {
    let (_s, pool) = server().await;
    ok(&pool, "CREATE TABLE rp (id INT PRIMARY KEY, v INT)").await;
    ok(&pool, "CREATE TABLE rc1 (p INT REFERENCES rp)").await;
    ok(&pool, "CREATE TABLE rc2 (p INT, FOREIGN KEY (p) REFERENCES rp)").await;
    ok(&pool, "CREATE TABLE rc3 (p INT)").await;
    ok(&pool, "ALTER TABLE rc3 ADD CONSTRAINT rc3_fk FOREIGN KEY (p) REFERENCES rp").await;
    // On itself: the primary key the same statement declares.
    ok(&pool, "CREATE TABLE tree (id INT PRIMARY KEY, parent INT REFERENCES tree)").await;

    for t in ["rc1", "rc2", "rc3"] {
        refused(&pool, &format!("INSERT INTO {t} VALUES (5)"), "23503", "is not present in table \"rp\"").await;
    }
    refused(&pool, "INSERT INTO tree VALUES (1, 9)", "23503", "is not present in table \"tree\"").await;

    ok(&pool, "INSERT INTO rp VALUES (5, 1)").await;
    for t in ["rc1", "rc2", "rc3"] {
        ok(&pool, &format!("INSERT INTO {t} VALUES (5)")).await;
    }
    ok(&pool, "INSERT INTO tree VALUES (1, NULL), (2, 1)").await;
    // The key is on `rp.id`: deleting the referenced row is refused.
    refused(&pool, "DELETE FROM rp WHERE id = 5", "23503", "foreign key").await;
}

#[tokio::test(flavor = "multi_thread")]
async fn references_without_a_column_needs_a_one_column_primary_key() {
    let (_s, pool) = server().await;
    ok(&pool, "CREATE TABLE nopk (id INT, v INT)").await;
    ok(&pool, "CREATE TABLE cpk (a INT, b INT, PRIMARY KEY (a, b))").await;
    refused(&pool, "CREATE TABLE n1 (p INT REFERENCES nopk)", "42704", "there is no primary key for referenced table \"nopk\"").await;
    refused(&pool, "CREATE TABLE n2 (p INT REFERENCES cpk)", "42830", "number of referencing and referenced columns").await;
    ok(&pool, "CREATE TABLE n3 (p INT)").await;
    refused(&pool, "ALTER TABLE n3 ADD CONSTRAINT n3_fk FOREIGN KEY (p) REFERENCES nopk", "42704", "no primary key").await;
    refused(&pool, "CREATE TABLE n4 (id INT, p INT REFERENCES n4)", "42704", "no primary key").await;
    for t in ["n1", "n2", "n4"] {
        assert!(!exists(&pool, t).await, "{t} must not have been created");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_foreign_key_onto_a_column_that_is_not_unique_is_refused() {
    let (_s, pool) = server().await;
    ok(&pool, "CREATE TABLE up (id INT PRIMARY KEY, v INT, u INT UNIQUE, k INT, UNIQUE (k))").await;
    refused(&pool, "CREATE TABLE u1 (p INT REFERENCES up (v))", "42830", "there is no unique constraint matching given keys for referenced table \"up\"").await;
    ok(&pool, "CREATE TABLE u2 (p INT)").await;
    refused(&pool, "ALTER TABLE u2 ADD CONSTRAINT u2_fk FOREIGN KEY (p) REFERENCES up (v)", "42830", "no unique constraint").await;
    // Each kind of uniqueness PostgreSQL accepts.
    ok(&pool, "CREATE TABLE u3 (p INT REFERENCES up (id))").await;
    ok(&pool, "CREATE TABLE u4 (p INT REFERENCES up (u))").await;
    ok(&pool, "CREATE TABLE u5 (p INT REFERENCES up (k))").await;
    assert!(!exists(&pool, "u1").await, "u1 must not have been created");
}
