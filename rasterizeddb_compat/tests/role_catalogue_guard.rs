//! The role catalogue, `rdb_roles`, cannot be dropped or altered: a script
//! that dropped every table in the default database dropped it, and every
//! login after that failed until a restart recreated it with the bootstrap
//! superuser alone. Refused as PostgreSQL refuses it on `pg_authid`: 42501,
//! "permission denied: … is a system catalog", with nothing changed, also
//! when it is one name among several.

use rasterizeddb_compat::TestServer;
use tokio_postgres::{Client, SimpleQueryMessage};

async fn client(server: &TestServer) -> Client {
    let (client, conn) = tokio_postgres::connect(&server.url(), tokio_postgres::NoTls)
        .await
        .expect("connect");
    tokio::spawn(conn);
    client
}

async fn rows(c: &Client, sql: &str) -> Vec<String> {
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

#[tokio::test(flavor = "multi_thread")]
async fn the_role_catalogue_is_neither_dropped_nor_altered() {
    let server = TestServer::start().await.expect("server");
    let c = client(&server).await;
    c.simple_query("CREATE ROLE keeper LOGIN PASSWORD 'pw'").await.expect("create a role");
    c.simple_query("CREATE TABLE t (a INT)").await.expect("create t");
    let before = rows(&c, "SELECT name FROM rdb_roles ORDER BY name").await;
    assert!(before.iter().any(|n| n == "keeper"), "{before:?}");
    for sql in [
        "DROP TABLE rdb_roles",
        "DROP TABLE IF EXISTS rdb_roles",
        "DROP TABLE t, rdb_roles",
        "ALTER TABLE rdb_roles ADD COLUMN x INT",
        "ALTER TABLE rdb_roles DROP COLUMN name",
        "ALTER TABLE rdb_roles RENAME COLUMN name TO n",
        "ALTER TABLE rdb_roles RENAME TO roles_gone",
    ] {
        let e = c.simple_query(sql).await.expect_err(sql);
        let db = e.as_db_error().unwrap_or_else(|| panic!("{sql}: {e}"));
        assert_eq!(db.code().code(), "42501", "{sql}: {}", db.message());
        assert_eq!(db.message(), "permission denied: \"rdb_roles\" is a system catalog", "{sql}");
    }
    assert_eq!(rows(&c, "SELECT name FROM rdb_roles ORDER BY name").await, before);
    // The other name in the refused DROP is still there.
    assert_eq!(rows(&c, "SELECT count(*) FROM t").await, ["0"]);
    c.simple_query("DROP TABLE t").await.expect("an ordinary table still drops");
}
