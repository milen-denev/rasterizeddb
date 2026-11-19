//! `GRANT`, `REVOKE`, `ALTER DEFAULT PRIVILEGES` and owners, as PostgreSQL 17
//! enforces them for a relation created from here on: its creator owns it,
//! and another non-superuser role needs a grant (42501). A relation created
//! before owners were recorded stays open to every role, as it was; a
//! superuser passes every check. GRANT was "syntax error or unsupported
//! query", and nothing was enforced for any role.

use rasterizeddb_compat::TestServer;
use rasterizeddb_core::auth::AuthMethod;
use rasterizeddb_core::pgwire::TlsMode;
use tokio_postgres::SimpleQueryMessage;

async fn as_user(server: &TestServer, user: &str, password: &str) -> tokio_postgres::Client {
    let (c, conn) = tokio_postgres::connect(&server.url_as(user, Some(password)), tokio_postgres::NoTls)
        .await
        .unwrap_or_else(|e| panic!("connect as {user}: {e}"));
    tokio::spawn(conn);
    c
}

async fn rows(c: &tokio_postgres::Client, sql: &str) -> Vec<String> {
    c.simple_query(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .into_iter()
        .filter_map(|m| match m {
            SimpleQueryMessage::Row(r) => {
                Some((0..r.len()).map(|i| r.get(i).unwrap_or("NULL").to_string()).collect::<Vec<_>>().join("|"))
            }
            _ => None,
        })
        .collect()
}

async fn code(c: &tokio_postgres::Client, sql: &str) -> String {
    let e = c.simple_query(sql).await.expect_err(sql);
    e.code().map(|s| s.code().to_string()).unwrap_or_else(|| panic!("{sql}: {e}"))
}

#[tokio::test(flavor = "multi_thread")]
async fn grants_decide_what_a_role_may_do_with_a_relation_it_does_not_own() {
    let server = TestServer::start_with(TlsMode::Disable, AuthMethod::ScramSha256, Some("boot")).await.expect("server");
    let admin = as_user(&server, "rasterized", "boot").await;
    admin
        .batch_execute(
            "CREATE TABLE legacy_free (a INT); \
             CREATE USER owner_role PASSWORD 'o'; CREATE USER app PASSWORD 'a'; CREATE USER other PASSWORD 'x'; \
             CREATE ROLE readers",
        )
        .await
        .expect("roles");
    // A table from before owners were recorded has no owner row.
    admin.batch_execute("DELETE FROM __rdb_acl WHERE relation = 'legacy_free'").await.expect("make it legacy");
    let owner = as_user(&server, "owner_role", "o").await;
    let app = as_user(&server, "app", "a").await;
    let other = as_user(&server, "other", "x").await;

    owner.batch_execute("CREATE TABLE t (id INT PRIMARY KEY, v TEXT); INSERT INTO t VALUES (1, 'a')").await.expect("owner creates");
    assert_eq!(rows(&owner, "SELECT v FROM t").await, ["a"]);
    // Not granted: refused, whatever the verb.
    for sql in ["SELECT * FROM t", "INSERT INTO t VALUES (2, 'b')", "UPDATE t SET v = 'z'", "DELETE FROM t", "TRUNCATE t", "DROP TABLE t", "ALTER TABLE t ADD COLUMN z INT"] {
        assert_eq!(code(&app, sql).await, "42501", "{sql}");
    }
    // A relation from before owners were recorded is open, as it was.
    app.batch_execute("INSERT INTO legacy_free VALUES (1)").await.expect("legacy open");

    owner.batch_execute("GRANT SELECT, INSERT ON t TO app").await.expect("grant");
    assert_eq!(rows(&app, "SELECT v FROM t").await, ["a"]);
    app.batch_execute("INSERT INTO t VALUES (2, 'b')").await.expect("granted insert");
    assert_eq!(code(&app, "UPDATE t SET v = 'z'").await, "42501");
    assert_eq!(code(&app, "GRANT SELECT ON t TO other").await, "42501");
    // Through a role.
    admin.batch_execute("GRANT readers TO other").await.expect("membership");
    owner.batch_execute("GRANT SELECT ON t TO readers").await.expect("grant to role");
    assert_eq!(rows(&other, "SELECT count(*) FROM t").await, ["2"]);
    // To PUBLIC.
    owner.batch_execute("CREATE TABLE p (a INT); GRANT SELECT ON p TO PUBLIC").await.expect("public");
    assert_eq!(rows(&app, "SELECT count(*) FROM p").await, ["0"]);
    // REVOKE takes it back.
    owner.batch_execute("REVOKE INSERT ON t FROM app").await.expect("revoke");
    assert_eq!(code(&app, "INSERT INTO t VALUES (3, 'c')").await, "42501");
    // WITH GRANT OPTION lets the grantee grant it on.
    owner.batch_execute("GRANT UPDATE ON t TO app WITH GRANT OPTION").await.expect("grant option");
    app.batch_execute("GRANT UPDATE ON t TO other").await.expect("granted on");
    other.batch_execute("UPDATE t SET v = 'o' WHERE id = 1").await.expect("other updates");
    // A view's privileges are the view's: its owner reads the table.
    owner.batch_execute("CREATE VIEW tv AS SELECT id FROM t; GRANT SELECT ON tv TO app").await.expect("view");
    owner.batch_execute("REVOKE SELECT ON t FROM app").await.expect("revoke table");
    assert_eq!(code(&app, "SELECT * FROM t").await, "42501");
    assert_eq!(rows(&app, "SELECT count(*) FROM tv").await, ["2"]);
    // A superuser passes; ownership moves with OWNER TO.
    assert_eq!(rows(&admin, "SELECT count(*) FROM t").await, ["2"]);
    admin.batch_execute("ALTER TABLE t OWNER TO app").await.expect("owner to");
    app.batch_execute("DELETE FROM t WHERE id = 2").await.expect("new owner");
    assert_eq!(code(&owner, "SELECT * FROM t").await, "42501");

    for (who, sql, expected) in [
        (&owner, "GRANT SELECT ON nosuch TO app", "42P01"),
        (&owner, "GRANT SELECT ON p TO nosuch", "42704"),
        (&owner, "GRANT USAGE ON p TO app", "0LP01"),
        (&app, "GRANT readers TO app", "42501"),
    ] {
        assert_eq!(code(who, sql).await, expected, "{sql}");
    }
    // Schemas, sequences and the rest are accepted.
    owner.batch_execute("GRANT USAGE ON SCHEMA public TO app; GRANT CONNECT ON DATABASE postgres TO app").await.expect("other objects");
}

#[tokio::test(flavor = "multi_thread")]
async fn default_privileges_apply_to_what_their_role_creates() {
    let server = TestServer::start_with(TlsMode::Disable, AuthMethod::ScramSha256, Some("boot")).await.expect("server");
    let admin = as_user(&server, "rasterized", "boot").await;
    admin.batch_execute("CREATE USER migrator PASSWORD 'm'; CREATE USER app PASSWORD 'a'").await.expect("roles");
    let migrator = as_user(&server, "migrator", "m").await;
    let app = as_user(&server, "app", "a").await;
    migrator
        .batch_execute("ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO app")
        .await
        .expect("defaults");
    migrator.batch_execute("CREATE TABLE orders (id INT PRIMARY KEY)").await.expect("create");
    app.batch_execute("INSERT INTO orders VALUES (1); UPDATE orders SET id = 2").await.expect("app writes");
    assert_eq!(rows(&app, "SELECT id FROM orders").await, ["2"]);
    assert_eq!(code(&app, "TRUNCATE orders").await, "42501");
    assert_eq!(code(&app, "DROP TABLE orders").await, "42501");
    // A block's DROP is checked too.
    assert_eq!(code(&app, "BEGIN; DROP TABLE orders").await, "42501");
}

/// A dropped table's grants go with it: a table created later under its
/// name is its new creator's, with no grant left over.
#[tokio::test(flavor = "multi_thread")]
async fn a_dropped_relations_grants_go_with_it() {
    let server = TestServer::start_with(TlsMode::Disable, AuthMethod::ScramSha256, Some("boot")).await.expect("server");
    let admin = as_user(&server, "rasterized", "boot").await;
    admin.batch_execute("CREATE USER o PASSWORD 'o'; CREATE USER app PASSWORD 'a'").await.expect("roles");
    let o = as_user(&server, "o", "o").await;
    let app = as_user(&server, "app", "a").await;
    o.batch_execute("CREATE TABLE t (a INT); GRANT SELECT ON t TO app; DROP TABLE t; CREATE TABLE t (a INT)").await.expect("recreate");
    assert_eq!(code(&app, "SELECT * FROM t").await, "42501");
    o.batch_execute("ALTER TABLE t RENAME TO t2; GRANT SELECT ON t2 TO app").await.expect("rename");
    assert_eq!(rows(&app, "SELECT count(*) FROM t2").await, ["0"]);
}
