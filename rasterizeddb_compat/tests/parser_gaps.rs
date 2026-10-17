//! Statements PostgreSQL accepts that the parser used to refuse: `DELETE`
//! with no `WHERE`, `CASE` branches holding arbitrary expressions, `IS [NOT]
//! DISTINCT FROM` against a boolean literal, and row-value equality.
//!
//! Every expected answer was taken from PostgreSQL 17 running the same
//! statements over the same rows, and is written out here rather than
//! recomputed, so a test cannot agree with a wrong engine by construction.

use rasterizeddb_compat::TestServer;
use sqlx::{AssertSqlSafe, Executor, Row};

async fn server() -> (TestServer, sqlx::PgPool) {
    let server = TestServer::start().await.expect("server should start");
    let pool = server.connect_sqlx().await.expect("should connect");
    (server, pool)
}

fn first_line(e: sqlx::Error) -> String {
    e.to_string().lines().next().unwrap_or("").to_string()
}

async fn run(pool: &sqlx::PgPool, sql: &str) -> Result<u64, String> {
    pool.execute(sqlx::raw_sql(AssertSqlSafe(sql.to_string())))
        .await
        .map(|r| r.rows_affected())
        .map_err(first_line)
}

/// The first column of every row, as the text PostgreSQL would print, with
/// `~` for NULL. Read over the simple protocol, so every value arrives as
/// text whatever its type.
async fn column(pool: &sqlx::PgPool, sql: &str) -> Result<Vec<String>, String> {
    let rows = pool
        .fetch_all(sqlx::raw_sql(AssertSqlSafe(sql.to_string())))
        .await
        .map_err(first_line)?;
    Ok(rows
        .iter()
        .map(|r| {
            r.try_get_unchecked::<Option<String>, _>(0)
                .expect("text")
                .unwrap_or_else(|| "~".to_string())
        })
        .collect())
}

async fn joined(pool: &sqlx::PgPool, sql: &str) -> Result<String, String> {
    column(pool, sql).await.map(|v| v.join(","))
}

// ---------------------------------------------------------------- #10 DELETE

/// `DELETE FROM t` with no `WHERE` was "Cannot parse DELETE query". It deletes
/// every row, reports the count in its command tag, rolls back with its
/// transaction, and takes `RETURNING` and an alias like any other `DELETE`.
#[tokio::test(flavor = "multi_thread")]
async fn delete_without_where_removes_every_row() {
    let (_s, pool) = server().await;
    run(&pool, "CREATE TABLE dd (id INT NOT NULL, v TEXT)").await.unwrap();
    let fill = "INSERT INTO dd (id, v) VALUES (1,'a'),(2,'b'),(3,NULL),(4,'d'),(5,'e')";
    run(&pool, fill).await.unwrap();

    // Inside a transaction that rolls back: gone inside, all back after.
    let mut tx = pool.begin().await.unwrap();
    let deleted = sqlx::query("DELETE FROM dd").execute(&mut *tx).await.expect("DELETE FROM dd");
    assert_eq!(deleted.rows_affected(), 5, "PostgreSQL's tag is DELETE 5");
    let inside: i64 = sqlx::query("SELECT count(*) FROM dd")
        .fetch_one(&mut *tx)
        .await
        .unwrap()
        .get(0);
    assert_eq!(inside, 0);
    tx.rollback().await.unwrap();
    assert_eq!(joined(&pool, "SELECT count(*) FROM dd").await.unwrap(), "5");

    // RETURNING reports every row as it was.
    let mut returned: Vec<(i32, Option<String>)> =
        sqlx::query("DELETE FROM dd RETURNING id, v")
            .fetch_all(&pool)
            .await
            .expect("DELETE … RETURNING")
            .iter()
            .map(|r| (r.get(0), r.get(1)))
            .collect();
    returned.sort();
    assert_eq!(
        returned,
        vec![
            (1, Some("a".into())),
            (2, Some("b".into())),
            (3, None),
            (4, Some("d".into())),
            (5, Some("e".into())),
        ]
    );
    assert_eq!(joined(&pool, "SELECT count(*) FROM dd").await.unwrap(), "0");

    // An empty table: DELETE 0, not an error.
    assert_eq!(run(&pool, "DELETE FROM dd").await, Ok(0));

    // Lower case, a quoted name and an alias, as PostgreSQL accepts them.
    run(&pool, "INSERT INTO dd (id, v) VALUES (1,'a'),(2,'b')").await.unwrap();
    assert_eq!(run(&pool, "delete from \"dd\" as x").await, Ok(2));
    run(&pool, "INSERT INTO dd (id, v) VALUES (1,'a'),(2,'b'),(3,'c')").await.unwrap();
    assert_eq!(run(&pool, "DELETE FROM dd x").await, Ok(3));
    run(&pool, "INSERT INTO dd (id, v) VALUES (1,'a')").await.unwrap();
    assert_eq!(run(&pool, "DELETE FROM dd;").await, Ok(1));

    // Text after the table that is neither an alias nor a clause is refused,
    // never read as "no condition": a statement the parser misread must not
    // become one that empties the table.
    run(&pool, "INSERT INTO dd (id, v) VALUES (1,'a'),(2,'b')").await.unwrap();
    assert!(run(&pool, "DELETE FROM dd x y").await.is_err());
    assert!(run(&pool, "DELETE FROM dd WHERE").await.is_err());
    assert_eq!(joined(&pool, "SELECT count(*) FROM dd").await.unwrap(), "2");
}

/// A whole-table delete of a parent runs each child's `ON DELETE` action for
/// every row it removes, and a blocking action refuses the whole statement.
#[tokio::test(flavor = "multi_thread")]
async fn delete_without_where_follows_foreign_keys() {
    let (_s, pool) = server().await;
    for ddl in [
        "CREATE TABLE par (id INT PRIMARY KEY)",
        "CREATE TABLE kid_c (id INT, pid INT REFERENCES par(id) ON DELETE CASCADE)",
        "CREATE TABLE kid_n (id INT, pid INT REFERENCES par(id) ON DELETE SET NULL)",
        "INSERT INTO par (id) VALUES (1),(2),(3)",
        "INSERT INTO kid_c (id, pid) VALUES (10,1),(11,2),(12,NULL)",
        "INSERT INTO kid_n (id, pid) VALUES (20,1),(21,3)",
    ] {
        run(&pool, ddl).await.unwrap_or_else(|e| panic!("{ddl}: {e}"));
    }
    assert_eq!(run(&pool, "DELETE FROM par").await, Ok(3));
    assert_eq!(joined(&pool, "SELECT count(*) FROM par").await.unwrap(), "0");
    // The child row with no parent stays; the two that had one cascade.
    assert_eq!(joined(&pool, "SELECT id FROM kid_c ORDER BY id").await.unwrap(), "12");
    assert_eq!(
        joined(&pool, "SELECT pid FROM kid_n ORDER BY id").await.unwrap(),
        "~,~"
    );

    for ddl in [
        "CREATE TABLE par2 (id INT PRIMARY KEY)",
        "CREATE TABLE kid_r (id INT, pid INT REFERENCES par2(id) ON DELETE RESTRICT)",
        "INSERT INTO par2 (id) VALUES (1),(2)",
        "INSERT INTO kid_r (id, pid) VALUES (30, 2)",
    ] {
        run(&pool, ddl).await.unwrap_or_else(|e| panic!("{ddl}: {e}"));
    }
    // PostgreSQL: 23503, and the unreferenced row 1 is not deleted either.
    let err = sqlx::query("DELETE FROM par2").execute(&pool).await.expect_err("RESTRICT");
    let code = err.as_database_error().and_then(|d| d.code()).map(|c| c.to_string());
    assert_eq!(code.as_deref(), Some("23503"), "{err}");
    assert_eq!(joined(&pool, "SELECT count(*) FROM par2").await.unwrap(), "2");
    assert_eq!(run(&pool, "DELETE FROM kid_r").await, Ok(1));
    assert_eq!(run(&pool, "DELETE FROM par2").await, Ok(2));
}

