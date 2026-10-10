//! `HAVING` as a boolean over aggregate comparisons.
//!
//! The clause used to be one comparison by construction: everything after the
//! first operator was taken as the literal, so `HAVING COUNT(*) > 1 AND SUM(x)
//! > 5` compared `COUNT(*)` against the text `1 AND SUM(x) > 5`. That text
//! does not coerce to a number, and an uncoercible literal *kept the group* —
//! so the statement returned every group and reported success.
//!
//! Every expectation here is written out longhand from the data, never taken
//! from the engine's own answer.

use rasterizeddb_compat::TestServer;
use sqlx::{AssertSqlSafe, Executor, Row};

async fn seeded() -> (TestServer, sqlx::PgPool) {
    let server = TestServer::start().await.expect("server should start");
    let pool = server.connect_sqlx().await.expect("should connect");
    for sql in [
        "CREATE TABLE hc (id INT PRIMARY KEY, g INT NOT NULL, status TEXT NOT NULL, x INT, price NUMERIC(10,2))",
        // g = 1: status a, x 1 2 3   -> count 3, sum 6,  max 3
        // g = 2: status b, x 10      -> count 1, sum 10, max 10
        // g = 3: status a, x 4 4     -> count 2, sum 8,  max 4
        // g = 4: status c, x NULL x2 -> count 2, sum NULL, max NULL
        // g = 5: status b, x 1       -> count 1, sum 1,  max 1
        "INSERT INTO hc (id, g, status, x, price) VALUES (1, 1, 'a', 1, 1.25)",
        "INSERT INTO hc (id, g, status, x, price) VALUES (2, 1, 'a', 2, 2.50)",
        "INSERT INTO hc (id, g, status, x, price) VALUES (3, 1, 'a', 3, 7.00)",
        "INSERT INTO hc (id, g, status, x, price) VALUES (4, 2, 'b', 10, 10.75)",
        "INSERT INTO hc (id, g, status, x, price) VALUES (5, 3, 'a', 4, 0.10)",
        "INSERT INTO hc (id, g, status, x, price) VALUES (6, 3, 'a', 4, 0.20)",
        "INSERT INTO hc (id, g, status, x, price) VALUES (7, 4, 'c', NULL, NULL)",
        "INSERT INTO hc (id, g, status, x, price) VALUES (8, 4, 'c', NULL, NULL)",
        "INSERT INTO hc (id, g, status, x, price) VALUES (9, 5, 'b', 1, 3.00)",
    ] {
        pool.execute(sqlx::raw_sql(AssertSqlSafe(sql.to_string())))
            .await
            .unwrap_or_else(|e| panic!("{}: {}", sql, e));
    }
    (server, pool)
}

async fn first_ints(pool: &sqlx::PgPool, sql: &str) -> Result<Vec<i64>, sqlx::Error> {
    let mut out: Vec<i64> = sqlx::query(AssertSqlSafe(sql.to_string()))
        .fetch_all(pool)
        .await?
        .iter()
        .map(|row| {
            row.try_get::<i32, _>(0)
                .map(i64::from)
                .unwrap_or_else(|_| row.get::<i64, _>(0))
        })
        .collect();
    out.sort();
    Ok(out)
}

#[tokio::test(flavor = "multi_thread")]
async fn compound_having_answers_exact_groups() {
    let (_s, pool) = seeded().await;
    let mut wrong = Vec::new();
    for (having, expected) in [
        ("COUNT(*) > 1 AND SUM(x) > 5", vec![1, 3]),
        ("COUNT(*) > 2 OR SUM(x) >= 10", vec![1, 2]),
        ("NOT (COUNT(*) = 1)", vec![1, 3, 4]),
        ("NOT COUNT(*) = 1", vec![1, 3, 4]),
        ("(COUNT(*) = 1 OR COUNT(*) = 3) AND SUM(x) < 10", vec![1, 5]),
        ("COUNT(*) = 1 OR COUNT(*) = 3 AND SUM(x) < 10", vec![1, 2, 5]),
        // Aggregate against aggregate.
        ("SUM(x) > COUNT(*)", vec![1, 2, 3]),
        ("MAX(x) = MIN(x)", vec![2, 3, 5]),
        // IS [NOT] NULL on an aggregate.
        ("MAX(x) IS NULL", vec![4]),
        ("MAX(x) IS NOT NULL", vec![1, 2, 3, 5]),
        ("SUM(x) IS NULL OR SUM(x) < 2", vec![4, 5]),
        // A NULL aggregate is unknown: neither the test nor its negation holds.
        ("NOT (SUM(x) > 5)", vec![5]),
        ("COUNT(*) BETWEEN 2 AND 3", vec![1, 3, 4]),
        ("COUNT(*) NOT BETWEEN 2 AND 3", vec![2, 5]),
        ("COUNT(*) IN (1, 3)", vec![1, 2, 5]),
        ("COUNT(*) NOT IN (1, 3)", vec![3, 4]),
        ("1 < COUNT(*)", vec![1, 3, 4]),
        // A fractional literal against an integer aggregate compares as a number.
        ("SUM(x) > 5.5", vec![1, 2, 3]),
        ("AVG(x) > 2.5", vec![2, 3]),
        ("SUM(price) > 10.5", vec![1, 2]),
        ("SUM(price) = 10.75", vec![1, 2]),
        ("COUNT(*) FILTER (WHERE x > 1) >= 2", vec![1, 3]),
        ("count(*) > 1\nAND\tsum(x) > 5", vec![1, 3]),
    ] {
        let sql = format!("SELECT g FROM hc GROUP BY g HAVING {}", having);
        match first_ints(&pool, &sql).await {
            Ok(got) if got == expected => {}
            Ok(got) => wrong.push(format!("HAVING {having}: got {got:?}, want {expected:?}")),
            Err(e) => wrong.push(format!("HAVING {having}: error {e}")),
        }
    }
    assert!(wrong.is_empty(), "{} wrong answers:\n{}", wrong.len(), wrong.join("\n"));
}

#[tokio::test(flavor = "multi_thread")]
async fn having_on_a_grouping_column_filters_groups() {
    let (_s, pool) = seeded().await;
    assert_eq!(
        first_ints(&pool, "SELECT g FROM hc GROUP BY g, status HAVING status = 'a'")
            .await
            .unwrap(),
        vec![1, 3]
    );
    assert_eq!(
        first_ints(
            &pool,
            "SELECT g FROM hc GROUP BY g, status HAVING status = 'a' AND COUNT(*) > 2"
        )
        .await
        .unwrap(),
        vec![1]
    );
    assert_eq!(
        first_ints(
            &pool,
            "SELECT g FROM hc WHERE id > 1 GROUP BY g, status HAVING status <> 'c' AND COUNT(*) >= 2"
        )
        .await
        .unwrap(),
        vec![1, 3]
    );
    let mut statuses: Vec<(String, i64)> = sqlx::query(
        "SELECT status, COUNT(*) FROM hc GROUP BY status HAVING status <> 'c' AND COUNT(*) >= 2",
    )
    .fetch_all(&pool)
    .await
    .unwrap()
    .iter()
    .map(|r| (r.get::<String, _>(0), r.get::<i64, _>(1)))
    .collect();
    statuses.sort();
    assert_eq!(statuses, vec![("a".to_string(), 5), ("b".to_string(), 2)]);

    // A plain column that is not grouped is refused, as PostgreSQL refuses it.
    assert!(
        first_ints(&pool, "SELECT g FROM hc GROUP BY g HAVING status = 'a'")
            .await
            .is_err()
    );
    // Mixed with an aggregate under OR: answered right, or refused.
    match first_ints(&pool, "SELECT g FROM hc GROUP BY g, status HAVING status = 'a' OR COUNT(*) > 1").await {
        Ok(got) => assert_eq!(got, vec![1, 3, 4]),
        Err(e) => eprintln!("refused: {e}"),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_having_that_cannot_be_read_is_an_error_not_every_group() {
    let (_s, pool) = seeded().await;
    for having in [
        "COUNT(*) > 'abc'",
        "SUM(x) > 1 AND",
        "COUNT(*) >",
        "COUNT(*) > 1 AND SUM(x) > 'many'",
        "SUM(price) > 'x'",
    ] {
        let sql = format!("SELECT g FROM hc GROUP BY g HAVING {}", having);
        let got = first_ints(&pool, &sql).await;
        assert!(got.is_err(), "HAVING {having} answered {got:?}");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn having_without_group_by_tests_the_whole_table() {
    let (_s, pool) = seeded().await;
    assert_eq!(
        first_ints(&pool, "SELECT COUNT(*) FROM hc HAVING COUNT(*) > 100")
            .await
            .unwrap(),
        Vec::<i64>::new()
    );
    assert_eq!(
        first_ints(&pool, "SELECT COUNT(*) FROM hc HAVING COUNT(*) > 1 AND MAX(x) = 10")
            .await
            .unwrap(),
        vec![9]
    );
    // No input rows is still one group, and HAVING decides whether it survives.
    assert_eq!(
        first_ints(&pool, "SELECT COUNT(*) FROM hc WHERE g = 99 HAVING COUNT(*) > 0")
            .await
            .unwrap(),
        Vec::<i64>::new()
    );
    assert_eq!(
        first_ints(&pool, "SELECT COUNT(*) FROM hc WHERE g = 99 HAVING COUNT(*) = 0")
            .await
            .unwrap(),
        vec![0]
    );
}

/// `GROUP BY g % 2` groups by the expression, not by the column it starts with.
#[tokio::test(flavor = "multi_thread")]
async fn an_expression_key_starting_with_a_column_is_not_that_column() {
    let (_s, pool) = seeded().await;
    assert_eq!(
        first_ints(&pool, "SELECT COUNT(*) FROM hc GROUP BY g % 2").await.unwrap(),
        vec![3, 6]
    );
    assert_eq!(
        first_ints(&pool, "SELECT COUNT(*) FROM hc GROUP BY g + 0").await.unwrap(),
        vec![1, 1, 2, 2, 3]
    );
}
