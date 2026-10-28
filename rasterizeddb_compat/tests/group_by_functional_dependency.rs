//! `GROUP BY` a unique, NOT NULL column makes the relation's other columns
//! functionally dependent on it, so they may be selected without being
//! grouped (PostgreSQL 9.1+). Django's `.annotate()` and Rails' `group(:id)`
//! emit exactly `SELECT t.id, t.name, COUNT(*) … GROUP BY t.id`, which was
//! refused with "must appear in GROUP BY".

use rasterizeddb_compat::TestServer;
use sqlx::{Executor, Row};

async fn seeded() -> (TestServer, sqlx::PgPool) {
    let server = TestServer::start().await.expect("server should start");
    let pool = server.connect_sqlx().await.expect("should connect");
    for sql in [
        "CREATE TABLE o (id INT PRIMARY KEY, customer TEXT NOT NULL, amount INT NOT NULL, note TEXT)",
        "INSERT INTO o (id, customer, amount, note) VALUES (1, 'a', 10, 'x'), (2, 'a', 20, NULL), (3, 'b', 5, 'y'), (4, 'c', 7, 'x')",
        "CREATE TABLE u (code TEXT UNIQUE NOT NULL, label TEXT NOT NULL, n INT NOT NULL)",
        "INSERT INTO u (code, label, n) VALUES ('p', 'first', 1), ('q', 'second', 2)",
        "CREATE TABLE nu (k INT UNIQUE, v TEXT)",
        "INSERT INTO nu (k, v) VALUES (1, 'one'), (NULL, 'n1'), (NULL, 'n2')",
    ] {
        pool.execute(sqlx::raw_sql(sqlx::AssertSqlSafe(sql.to_string())))
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
    (server, pool)
}

#[tokio::test(flavor = "multi_thread")]
async fn columns_of_a_grouped_primary_key_are_dependent() {
    let (_s, pool) = seeded().await;
    let mut rows: Vec<(i32, String, Option<String>, i64, i64)> = sqlx::query(
        "SELECT id, customer, note, COUNT(*), SUM(amount) FROM o GROUP BY id",
    )
    .fetch_all(&pool)
    .await
    .unwrap()
    .iter()
    .map(|r| (r.get(0), r.get(1), r.get(2), r.get(3), r.get(4)))
    .collect();
    rows.sort();
    assert_eq!(
        rows,
        vec![
            (1, "a".into(), Some("x".into()), 1, 10),
            (2, "a".into(), None, 1, 20),
            (3, "b".into(), Some("y".into()), 1, 5),
            (4, "c".into(), Some("x".into()), 1, 7),
        ]
    );

    // A UNIQUE NOT NULL column determines the row just as a primary key does.
    let mut rows: Vec<(String, String, i64)> =
        sqlx::query("SELECT code, label, SUM(n) FROM u GROUP BY code")
            .fetch_all(&pool)
            .await
            .unwrap()
            .iter()
            .map(|r| (r.get(0), r.get(1), r.get(2)))
            .collect();
    rows.sort();
    assert_eq!(
        rows,
        vec![("p".into(), "first".into(), 1), ("q".into(), "second".into(), 2)]
    );

    // HAVING over a dependent column, and ordering by one.
    let ids: Vec<i32> = sqlx::query(
        "SELECT id, customer FROM o GROUP BY id HAVING customer = 'a' AND COUNT(*) = 1 ORDER BY id",
    )
    .fetch_all(&pool)
    .await
    .unwrap()
    .iter()
    .map(|r| r.get(0))
    .collect();
    assert_eq!(ids, vec![1, 2]);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_column_that_does_not_determine_the_row_is_still_refused() {
    let (_s, pool) = seeded().await;
    for sql in [
        // Not unique.
        "SELECT customer, amount, COUNT(*) FROM o GROUP BY customer",
        // Unique but nullable: every NULL is its own row to the constraint and
        // one group to the fold.
        "SELECT k, v, COUNT(*) FROM nu GROUP BY k",
        // Grouped by an expression of the key, not the key.
        "SELECT id + 0, customer, COUNT(*) FROM o GROUP BY id + 0",
    ] {
        assert!(
            sqlx::query(sqlx::AssertSqlSafe(sql)).fetch_all(&pool).await.is_err(),
            "{sql} was answered"
        );
    }
}

/// `GROUP BY 1` groups by the first select item, as reporting tools and ORMs
/// write it; it was "GROUP BY column '1' not found in schema".
#[tokio::test(flavor = "multi_thread")]
async fn a_number_groups_by_that_select_item() {
    let (_s, pool) = seeded().await;
    let mut rows: Vec<(String, i64)> = sqlx::query("SELECT customer, COUNT(*) FROM o GROUP BY 1")
        .fetch_all(&pool)
        .await
        .unwrap()
        .iter()
        .map(|r| (r.get(0), r.get(1)))
        .collect();
    rows.sort();
    assert_eq!(rows, vec![("a".into(), 2), ("b".into(), 1), ("c".into(), 1)]);

    // An expression item, and an alias beside it.
    let mut rows: Vec<(String, i64)> = sqlx::query("SELECT upper(customer) AS c, SUM(amount) AS s FROM o GROUP BY 1")
        .fetch_all(&pool)
        .await
        .unwrap()
        .iter()
        .map(|r| (r.get(0), r.get(1)))
        .collect();
    rows.sort();
    assert_eq!(rows, vec![("A".into(), 30), ("B".into(), 5), ("C".into(), 7)]);

    for sql in ["SELECT customer, COUNT(*) FROM o GROUP BY 3", "SELECT customer FROM o GROUP BY 0"] {
        let e = sqlx::query(sql).fetch_all(&pool).await.expect_err(sql);
        let code = e.as_database_error().and_then(|d| d.code()).map(|c| c.to_string());
        assert_eq!(code.as_deref(), Some("42P10"), "{sql}: {e}");
    }
    // An aggregate's position is refused, as PostgreSQL refuses it.
    assert!(sqlx::query("SELECT customer, COUNT(*) FROM o GROUP BY 2").fetch_all(&pool).await.is_err());
}
