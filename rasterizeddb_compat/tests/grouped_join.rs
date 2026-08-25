//! Aggregates over a join, in the shape the application's advice rollup uses:
//! qualified grouping keys and a cast around the aggregate —
//! `SELECT a.user_table_id, SUM(a.slow_calls)::bigint … FROM … a JOIN …
//! GROUP BY a.user_table_id, …`.

use rasterizeddb_compat::TestServer;
use sqlx::Row;

async fn server() -> (TestServer, sqlx::PgPool) {
    let server = TestServer::start().await.expect("server should start");
    let pool = server.connect_sqlx().await.expect("should connect");
    (server, pool)
}

async fn run(pool: &sqlx::PgPool, sql: &str) -> Result<(), String> {
    sqlx::query(sql)
        .execute(pool)
        .await
        .map(|_| ())
        .map_err(|e| e.to_string().lines().next().unwrap_or("").to_string())
}

#[tokio::test]
async fn a_grouped_join_takes_a_cast_around_its_aggregate() {
    let (_s, pool) = server().await;
    run(&pool, "CREATE TABLE advice (user_table_id INT NOT NULL, domain_id INT NOT NULL, slow_calls INT NOT NULL)")
        .await
        .expect("create advice");
    run(&pool, "CREATE TABLE ut (id INT NOT NULL, name TEXT NOT NULL)")
        .await
        .expect("create ut");
    for (u, d, s) in [(1, 10, 5), (1, 10, 7), (2, 10, 1)] {
        run(&pool, &format!("INSERT INTO advice (user_table_id, domain_id, slow_calls) VALUES ({}, {}, {})", u, d, s))
            .await
            .expect("insert advice");
    }
    run(&pool, "INSERT INTO ut (id, name) VALUES (1, 'a')").await.expect("i");
    run(&pool, "INSERT INTO ut (id, name) VALUES (2, 'b')").await.expect("i");

    // The rollup shape: qualified group keys, a cast wrapping the aggregate.
    let rows = sqlx::query(
        "SELECT a.user_table_id AS user_table_id, a.domain_id AS domain_id, \
         SUM(a.slow_calls)::bigint AS slow_calls \
         FROM advice a JOIN ut u ON u.id = a.user_table_id \
         GROUP BY a.user_table_id, a.domain_id \
         ORDER BY user_table_id",
    )
    .fetch_all(&pool)
    .await
    .expect("the grouped join must work");
    assert_eq!(rows.len(), 2, "two groups");
    assert_eq!(rows[0].try_get::<i64, _>("slow_calls").expect("sum"), 12);
    assert_eq!(rows[1].try_get::<i64, _>("slow_calls").expect("sum"), 1);
    assert_eq!(rows[0].try_get::<i32, _>("user_table_id").expect("key"), 1);
}

/// Variants of the same rollup, closing in on the production statement: no
/// aliases on the select items, LEFT JOIN, a WHERE, extra aggregates.
#[tokio::test]
async fn rollup_variants() {
    let (_s, pool) = server().await;
    run(&pool, "CREATE TABLE advice (user_table_id INT NOT NULL, domain_id INT NOT NULL, slow_calls INT NOT NULL)")
        .await
        .expect("create advice");
    run(&pool, "CREATE TABLE ut (id INT NOT NULL, name TEXT NOT NULL)")
        .await
        .expect("create ut");
    for (u, d, s) in [(1, 10, 5), (1, 10, 7), (2, 10, 1)] {
        run(&pool, &format!("INSERT INTO advice (user_table_id, domain_id, slow_calls) VALUES ({}, {}, {})", u, d, s))
            .await
            .expect("insert advice");
    }
    run(&pool, "INSERT INTO ut (id, name) VALUES (1, 'a')").await.expect("i");
    run(&pool, "INSERT INTO ut (id, name) VALUES (2, 'b')").await.expect("i");

    let mut failures: Vec<String> = Vec::new();
    for (label, sql) in [
        ("no aliases", "SELECT a.user_table_id, a.domain_id, SUM(a.slow_calls)::bigint FROM advice a JOIN ut u ON u.id = a.user_table_id GROUP BY a.user_table_id, a.domain_id"),
        ("left join", "SELECT a.user_table_id, SUM(a.slow_calls)::bigint AS s FROM advice a LEFT JOIN ut u ON u.id = a.user_table_id GROUP BY a.user_table_id"),
        ("with where", "SELECT a.user_table_id, SUM(a.slow_calls)::bigint AS s FROM advice a JOIN ut u ON u.id = a.user_table_id WHERE a.domain_id = 10 GROUP BY a.user_table_id"),
        ("mixed aggregates", "SELECT a.user_table_id, SUM(a.slow_calls)::bigint AS s, COUNT(*) AS n, MAX(a.slow_calls) AS m FROM advice a JOIN ut u ON u.id = a.user_table_id GROUP BY a.user_table_id"),
        ("joined column in select", "SELECT a.user_table_id, u.name, SUM(a.slow_calls)::bigint AS s FROM advice a JOIN ut u ON u.id = a.user_table_id GROUP BY a.user_table_id, u.name"),
        ("order by aggregate", "SELECT a.user_table_id, SUM(a.slow_calls)::bigint AS s FROM advice a JOIN ut u ON u.id = a.user_table_id GROUP BY a.user_table_id ORDER BY s DESC"),
        ("cast of count", "SELECT a.user_table_id, COUNT(*)::int AS n FROM advice a JOIN ut u ON u.id = a.user_table_id GROUP BY a.user_table_id"),
        ("avg cast real", "SELECT a.user_table_id, AVG(a.slow_calls)::real AS r FROM advice a JOIN ut u ON u.id = a.user_table_id GROUP BY a.user_table_id"),
    ] {
        if let Err(e) = sqlx::query(sql).fetch_all(&pool).await {
            failures.push(format!("{}: {}", label, e.to_string().lines().next().unwrap_or("")));
        }
    }
    assert!(failures.is_empty(), "variants that failed:\n{}", failures.join("\n"));
}

/// The Auto-tier promotion worker, verbatim in shape: the select list carries
/// `SUM(x)::bigint AS alias`, and `ORDER BY SUM(x)` names the aggregate by its
/// call rather than by the alias. The cast makes that select item a computed
/// expression rather than a bare aggregate, so a matcher that knew only
/// labels and bare aggregates refused a statement whose select list plainly
/// carried the term — "ORDER BY term 'SUM(a.slow_calls)' is not in the SELECT
/// list". This worker failed every cycle.
#[tokio::test]
async fn order_by_names_a_cast_wrapped_aggregate() {
    let (_s, pool) = server().await;
    run(&pool, "CREATE TABLE advice (user_table_id INT NOT NULL, domain_id INT NOT NULL, slow_calls INT NOT NULL)")
        .await
        .expect("create advice");
    run(&pool, "CREATE TABLE ut (id INT NOT NULL, name TEXT NOT NULL)")
        .await
        .expect("create ut");
    for (u, d, s) in [(1, 10, 5), (1, 10, 7), (2, 10, 30), (3, 10, 1)] {
        run(&pool, &format!("INSERT INTO advice (user_table_id, domain_id, slow_calls) VALUES ({}, {}, {})", u, d, s))
            .await
            .expect("insert advice");
    }
    for id in 1..=3 {
        run(&pool, &format!("INSERT INTO ut (id, name) VALUES ({}, 'n{}')", id, id))
            .await
            .expect("insert ut");
    }

    let rows = sqlx::query(
        "SELECT a.user_table_id AS user_table_id, a.domain_id AS domain_id, \
         SUM(a.slow_calls)::bigint AS slow_calls \
         FROM advice a JOIN ut t ON t.id = a.user_table_id \
         GROUP BY a.user_table_id, a.domain_id \
         HAVING SUM(a.slow_calls) >= $1 \
         ORDER BY SUM(a.slow_calls) DESC LIMIT $2",
    )
    .bind(10i64)
    .bind(10i64)
    .fetch_all(&pool)
    .await
    .expect("ORDER BY a cast-wrapped aggregate must resolve");

    // Table 2 sums to 30, table 1 to 12; table 3's single slow call is below
    // the HAVING threshold and must not appear.
    let got: Vec<(i32, i64)> = rows
        .iter()
        .map(|r| {
            (
                r.try_get::<i32, _>("user_table_id").expect("id"),
                r.try_get::<i64, _>("slow_calls").expect("sum"),
            )
        })
        .collect();
    assert_eq!(got, vec![(2, 30), (1, 12)], "ordered by the summed value, descending");

    // The alias still resolves, and so does the argument-free form.
    let rows = sqlx::query(
        "SELECT a.user_table_id AS user_table_id, SUM(a.slow_calls)::bigint AS slow_calls, COUNT(*) AS n \
         FROM advice a JOIN ut t ON t.id = a.user_table_id \
         GROUP BY a.user_table_id ORDER BY slow_calls DESC",
    )
    .fetch_all(&pool)
    .await
    .expect("ordering by the alias still works");
    assert_eq!(rows[0].try_get::<i32, _>("user_table_id").expect("id"), 2);

    let rows = sqlx::query(
        "SELECT a.user_table_id AS user_table_id, COUNT(*) AS n \
         FROM advice a JOIN ut t ON t.id = a.user_table_id \
         GROUP BY a.user_table_id ORDER BY COUNT(*) DESC, a.user_table_id",
    )
    .fetch_all(&pool)
    .await
    .expect("ORDER BY COUNT(*) still works");
    assert_eq!(rows[0].try_get::<i64, _>("n").expect("n"), 2, "table 1 has two rows");
}
