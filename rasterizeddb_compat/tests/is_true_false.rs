//! `IS [NOT] TRUE`, `IS [NOT] FALSE`, `IS [NOT] UNKNOWN` with PostgreSQL's
//! three-valued meaning: a NULL is neither true nor false, so `IS NOT TRUE`
//! and `IS NOT FALSE` hold for it. These were a parse error ("IS must be
//! followed by NULL …"). Expected rows are written out from the data.

use rasterizeddb_compat::TestServer;
use sqlx::{AssertSqlSafe, Executor, Row};

#[tokio::test(flavor = "multi_thread")]
async fn truth_tests_follow_three_valued_logic() {
    let server = TestServer::start().await.expect("server should start");
    let pool = server.connect_sqlx().await.expect("should connect");
    pool.execute("CREATE TABLE b (id INT NOT NULL, flag BOOLEAN, nn BOOLEAN NOT NULL, n INT)")
        .await
        .unwrap();
    pool.execute(
        "INSERT INTO b (id, flag, nn, n) VALUES (1, true, true, 5), (2, false, false, 10), (3, NULL, true, NULL), (4, true, false, 1)",
    )
    .await
    .unwrap();
    let mut wrong = Vec::new();
    for (predicate, expected) in [
        ("flag IS TRUE", vec![1, 4]),
        ("flag IS NOT TRUE", vec![2, 3]),
        ("flag IS FALSE", vec![2]),
        ("flag IS NOT FALSE", vec![1, 3, 4]),
        ("flag IS UNKNOWN", vec![3]),
        ("flag IS NOT UNKNOWN", vec![1, 2, 4]),
        ("flag is not true", vec![2, 3]),
        ("nn IS TRUE", vec![1, 3]),
        ("nn IS NOT TRUE", vec![2, 4]),
        ("nn IS FALSE", vec![2, 4]),
        ("nn IS NOT FALSE", vec![1, 3]),
        ("nn IS UNKNOWN", vec![]),
        ("nn IS NOT UNKNOWN", vec![1, 2, 3, 4]),
        ("(n > 4) IS TRUE", vec![1, 2]),
        ("(n > 4) IS NOT TRUE", vec![3, 4]),
        ("(n > 4) IS FALSE", vec![4]),
        ("(n > 4) IS NOT FALSE", vec![1, 2, 3]),
        ("(n > 4) IS UNKNOWN", vec![3]),
        ("(n > 4) IS NOT UNKNOWN", vec![1, 2, 4]),
        ("(n = 5) IS NOT TRUE", vec![2, 3, 4]),
        ("flag IS NOT TRUE AND id > 2", vec![3]),
        ("flag IS TRUE OR nn IS FALSE", vec![1, 2, 4]),
    ] {
        let got = sqlx::query(AssertSqlSafe(format!("SELECT id FROM b WHERE {predicate} ORDER BY id")))
            .fetch_all(&pool)
            .await
            .map(|rows| rows.iter().map(|r| r.get::<i32, _>(0)).collect::<Vec<_>>());
        match got {
            Ok(ids) if ids == expected => {}
            Ok(ids) => wrong.push(format!("{predicate}: got {ids:?}, want {expected:?}")),
            Err(e) => wrong.push(format!("{predicate}: error {e}")),
        }
    }
    assert!(wrong.is_empty(), "{} wrong answers:\n{}", wrong.len(), wrong.join("\n"));

    // Not a boolean: refused, as PostgreSQL refuses it.
    assert!(sqlx::query("SELECT id FROM b WHERE n IS TRUE").fetch_all(&pool).await.is_err());

    // In a CASE, through the expression evaluator.
    let mut rows: Vec<(i32, i64)> = sqlx::query(
        "SELECT id, CASE WHEN flag IS NOT TRUE THEN 1 ELSE 0 END AS c FROM b",
    )
    .fetch_all(&pool)
    .await
    .unwrap()
    .iter()
    .map(|r| {
        let c = r.try_get::<i64, _>(1).unwrap_or_else(|_| i64::from(r.get::<i32, _>(1)));
        (r.get::<i32, _>(0), c)
    })
    .collect();
    rows.sort();
    assert_eq!(rows, vec![(1, 0), (2, 1), (3, 1), (4, 0)]);
}

/// `( <expression> ) IS [NOT] TRUE | FALSE | UNKNOWN` over AND/OR/NOT, a
/// column on both sides and arithmetic, in three-valued logic. Every expected
/// set is worked out by hand from the rows, row by row.
#[tokio::test(flavor = "multi_thread")]
async fn truth_tests_over_general_expressions() {
    let server = TestServer::start().await.expect("server should start");
    let pool = server.connect_sqlx().await.expect("should connect");
    pool.execute("CREATE TABLE e (id INT NOT NULL, a INT, b INT, g BOOLEAN)")
        .await
        .unwrap();
    // a > b:  F, T, U, U, F        g: T, F, T, U, U
    pool.execute(
        "INSERT INTO e (id, a, b, g) VALUES (1, 1, 2, true), (2, 3, 2, false), (3, NULL, 2, true), (4, 1, NULL, NULL), (5, 2, 2, NULL)",
    )
    .await
    .unwrap();
    let mut wrong = Vec::new();
    for (predicate, expected) in [
        ("(a > b) IS TRUE", vec![2]),
        ("(a > b) IS NOT TRUE", vec![1, 3, 4, 5]),
        ("(a > b) IS FALSE", vec![1, 5]),
        ("(a > b) IS NOT FALSE", vec![2, 3, 4]),
        ("(a > b) IS UNKNOWN", vec![3, 4]),
        ("(a > b) IS NOT UNKNOWN", vec![1, 2, 5]),
        ("(a > b AND g) IS TRUE", vec![]),
        ("(a > b AND g) IS FALSE", vec![1, 2, 5]),
        ("(a > b AND g) IS UNKNOWN", vec![3, 4]),
        ("(a > b AND g) IS NOT TRUE", vec![1, 2, 3, 4, 5]),
        ("(a > b OR g) IS TRUE", vec![1, 2, 3]),
        ("(a > b OR g) IS FALSE", vec![]),
        ("(a > b OR g) IS UNKNOWN", vec![4, 5]),
        ("(a > b OR g) IS NOT TRUE", vec![4, 5]),
        ("(a > b OR g) IS NOT FALSE", vec![1, 2, 3, 4, 5]),
        ("(NOT (a = b)) IS TRUE", vec![1, 2]),
        ("(NOT a = b) IS FALSE", vec![5]),
        ("(NOT (a = b)) IS UNKNOWN", vec![3, 4]),
        ("(a + 1 >= b OR NOT g) IS TRUE", vec![1, 2, 5]),
        ("(a + 1 >= b OR NOT g) IS UNKNOWN", vec![3, 4]),
        ("(a + 1 >= b OR NOT g) IS FALSE", vec![]),
        ("((a > b) OR (b = 2 AND g)) IS TRUE", vec![1, 2, 3]),
        ("(a > b) IS NOT TRUE AND id < 4", vec![1, 3]),
        // A comparison with NULL is unknown under a truth test, not false.
        ("(a = NULL) IS UNKNOWN", vec![1, 2, 3, 4, 5]),
        ("(a = NULL) IS NOT FALSE", vec![1, 2, 3, 4, 5]),
        ("(a = NULL) IS FALSE", vec![]),
        ("(a = NULL OR g) IS TRUE", vec![1, 3]),
        // b = 2 holds wherever b is set, so AND-ing an unknown keeps it unknown;
        // b = 3 is false there, which decides the AND.
        ("(b = 2 AND a <> NULL) IS NOT UNKNOWN", vec![]),
        ("(b = 3 AND a <> NULL) IS NOT UNKNOWN", vec![1, 2, 3, 5]),
        // A function call is evaluated, as in the select list: |a| is 1, 3,
        // NULL, 1, 2, so the NULL row is unknown, not false.
        ("(abs(a) = 1) IS TRUE", vec![1, 4]),
        ("(abs(a) = 1) IS FALSE", vec![2, 5]),
        ("(abs(a) = 1) IS UNKNOWN", vec![3]),
        ("(abs(a) = 1) IS NOT TRUE", vec![2, 3, 5]),
    ] {
        let got = sqlx::query(AssertSqlSafe(format!("SELECT id FROM e WHERE {predicate} ORDER BY id")))
            .fetch_all(&pool)
            .await
            .map(|rows| rows.iter().map(|r| r.get::<i32, _>(0)).collect::<Vec<_>>());
        match got {
            Ok(ids) if ids == expected => {}
            Ok(ids) => wrong.push(format!("{predicate}: got {ids:?}, want {expected:?}")),
            Err(e) => wrong.push(format!("{predicate}: error {e}")),
        }
    }
    assert!(wrong.is_empty(), "{} wrong answers:\n{}", wrong.len(), wrong.join("\n"));

    // What the expansion cannot express is refused, never approximated.
    for predicate in ["(a IN (1, 2)) IS TRUE", "(a LIKE 'x%') IS FALSE"] {
        let got = sqlx::query(AssertSqlSafe(format!("SELECT id FROM e WHERE {predicate}")))
            .fetch_all(&pool)
            .await;
        assert!(got.is_err(), "{predicate} answered {} rows", got.map(|r| r.len()).unwrap_or(0));
    }
}
