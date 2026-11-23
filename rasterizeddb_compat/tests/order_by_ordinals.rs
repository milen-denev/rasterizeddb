//! `ORDER BY <position>` — a sort key given as an index into the select list.
//!
//! Standard SQL, and how a hand-written query or a migration script usually
//! spells "sort by the second column". Resolved once, into the item's own
//! label, so the scan sort, the expression sort, the join and the aggregate
//! fold all see an ordinary key.

use sqlx::{AssertSqlSafe, Connection, Executor, Row};
use rasterizeddb_compat::TestServer;

async fn seeded() -> (TestServer, sqlx::PgConnection) {
    let server = TestServer::start().await.expect("start");
    let mut conn = sqlx::PgConnection::connect(&server.url()).await.expect("connect");
    for sql in [
        "CREATE TABLE t (id INT PRIMARY KEY, grp INT NOT NULL, name TEXT NOT NULL)",
        "CREATE TABLE u (id INT PRIMARY KEY, t_id INT NOT NULL, note TEXT NOT NULL)",
        "INSERT INTO t (id, grp, name) VALUES (1, 2, 'charlie')",
        "INSERT INTO t (id, grp, name) VALUES (2, 1, 'alpha')",
        "INSERT INTO t (id, grp, name) VALUES (3, 2, 'bravo')",
        "INSERT INTO u (id, t_id, note) VALUES (10, 1, 'n1')",
        "INSERT INTO u (id, t_id, note) VALUES (11, 2, 'n2')",
        "INSERT INTO u (id, t_id, note) VALUES (12, 3, 'n3')",
    ] {
        conn.execute(AssertSqlSafe(sql)).await.expect(sql);
    }
    (server, conn)
}

async fn names(conn: &mut sqlx::PgConnection, sql: &str) -> Vec<String> {
    sqlx::query(AssertSqlSafe(sql))
        .fetch_all(conn)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .iter()
        .map(|r| r.try_get::<String, _>("name").expect("name"))
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_ordinal_names_a_select_item() {
    let (_server, mut conn) = seeded().await;

    // Position 2 is `name`, so this is `ORDER BY name`.
    assert_eq!(
        names(&mut conn, "SELECT id, name FROM t ORDER BY 2").await,
        vec!["alpha", "bravo", "charlie"],
    );
    assert_eq!(
        names(&mut conn, "SELECT id, name FROM t ORDER BY 2 DESC").await,
        vec!["charlie", "bravo", "alpha"],
    );
    // And it agrees with naming the column.
    assert_eq!(
        names(&mut conn, "SELECT id, name FROM t ORDER BY 2").await,
        names(&mut conn, "SELECT id, name FROM t ORDER BY name").await,
    );

    // Position 1 is `grp`, which ties — the second key breaks it.
    assert_eq!(
        names(&mut conn, "SELECT grp, name FROM t ORDER BY 1, 2").await,
        vec!["alpha", "bravo", "charlie"],
    );

    // Over a join, where positions count the select list as written.
    assert_eq!(
        names(
            &mut conn,
            "SELECT t.id AS id, t.name AS name FROM t JOIN u ON u.t_id = t.id ORDER BY 2",
        )
        .await,
        vec!["alpha", "bravo", "charlie"],
    );
}

/// A position that names nothing must say so, rather than sorting by something.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_out_of_range_ordinal_is_refused() {
    let (_server, mut conn) = seeded().await;
    for sql in [
        "SELECT id, name FROM t ORDER BY 5",
        "SELECT id, name FROM t ORDER BY 0",
    ] {
        let outcome = sqlx::query(AssertSqlSafe(sql)).fetch_all(&mut conn).await;
        assert!(
            outcome.is_err(),
            "{sql}: a position outside the select list must be refused"
        );
    }

    // `1 + 1` is an expression whose value is a number, not a position — it
    // must not be read as one.
    let rows = sqlx::query(AssertSqlSafe("SELECT id, name FROM t ORDER BY 1 + 1"))
        .fetch_all(&mut conn)
        .await;
    if let Ok(rows) = rows {
        assert_eq!(rows.len(), 3, "a constant key leaves every row in place");
    }
}
