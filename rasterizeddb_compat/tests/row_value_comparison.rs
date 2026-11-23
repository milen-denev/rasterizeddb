//! `(a, b) > (c, d)` — SQL's row-value comparison.
//!
//! It was a parse error: `could not compile the query plan: expected closing
//! parenthesis`. That message is why it stayed hidden — it reads as a quoting
//! mistake in the caller's own SQL, and sent two people looking for one.
//!
//! It is the canonical keyset-pagination idiom, so the way it failed was worse
//! than the gap itself. A keyset page has no cursor on the first page, so page
//! one carries no tuple and succeeds; every page after it carries the cursor
//! and fails. A migration would start, die on page two, and present as "the
//! save silently does nothing" — while a table small enough to fit in one page
//! worked perfectly.
//!
//! It was also looked at directly and cleared: asked "is this what is wedging
//! the server?", the answer was no — it returns immediately. True, and the
//! wrong conclusion, because it returns *an error*. A test that answers the
//! question you asked can still leave the bug standing.

use rasterizeddb_compat::TestServer;
use sqlx::{AssertSqlSafe, Row};

async fn seeded() -> (TestServer, sqlx::PgPool) {
    let server = TestServer::start().await.expect("start");
    let pool = server.connect_sqlx().await.expect("connect");
    for sql in [
        "CREATE TABLE kp (a INT, b VARCHAR)",
        "INSERT INTO kp (a,b) VALUES (1,'x'),(1,'y'),(2,'a'),(2,'b'),(3,'c')",
    ] {
        sqlx::query(AssertSqlSafe(sql)).execute(&pool).await.expect(sql);
    }
    (server, pool)
}

async fn pairs(pool: &sqlx::PgPool, sql: &str) -> Vec<String> {
    sqlx::query(AssertSqlSafe(sql))
        .fetch_all(pool)
        .await
        .unwrap_or_else(|e| panic!("{sql}\nfailed: {e}"))
        .into_iter()
        .map(|r| {
            format!(
                "{}{}",
                r.try_get::<i32, _>(0).expect("a"),
                r.try_get::<String, _>(1).expect("b")
            )
        })
        .collect()
}

/// Every operator, against a hand-computed answer.
///
/// The orderings are checked rather than just the row counts: an expansion that
/// compared only the first column would return the right number of rows for
/// some of these and the wrong rows.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn row_value_comparisons_order_by_the_whole_tuple() {
    let (_server, pool) = seeded().await;

    for (sql, expected) in [
        (
            "SELECT a,b FROM kp WHERE (a, b) > (1, 'x') ORDER BY a, b",
            vec!["1y", "2a", "2b", "3c"],
        ),
        (
            "SELECT a,b FROM kp WHERE (a, b) >= (2, 'a') ORDER BY a, b",
            vec!["2a", "2b", "3c"],
        ),
        ("SELECT a,b FROM kp WHERE (a, b) < (2, 'a') ORDER BY a, b", vec!["1x", "1y"]),
        (
            "SELECT a,b FROM kp WHERE (a, b) <= (2, 'a') ORDER BY a, b",
            vec!["1x", "1y", "2a"],
        ),
        ("SELECT a,b FROM kp WHERE (a, b) = (2, 'a') ORDER BY a, b", vec!["2a"]),
        (
            "SELECT a,b FROM kp WHERE (a, b) <> (2, 'a') ORDER BY a, b",
            vec!["1x", "1y", "2b", "3c"],
        ),
    ] {
        assert_eq!(pairs(&pool, sql).await, expected, "{sql}");
    }

    // The idiom in the shape that actually broke: a cursor plus a limit.
    assert_eq!(
        pairs(
            &pool,
            "SELECT a,b FROM kp WHERE (a, b) > (1, 'y') ORDER BY a, b LIMIT 2",
        )
        .await,
        vec!["2a", "2b"],
        "keyset page after a cursor"
    );
}

/// Shapes that merely look like row comparisons and must be left alone.
///
/// The dangerous one is the function call: `substring(x, 1)` has a top-level
/// comma, so without a guard `substring(x, 1) > (y)` is read as a two-value
/// tuple compared to a one-value tuple, and a working query becomes an arity
/// error. `opens_a_call` in `predicate_rewrite` is what prevents it. No
/// two-argument function is currently callable from a `WHERE` clause here, so
/// the guard cannot be exercised end to end yet — it is defensive against the
/// first one that is.
///
/// Separately, and *not* caused by the row-value work: a parenthesised **bare
/// column** on the left of a comparison — `WHERE (a) > 1` — is rejected with
/// "unexpected Op(\">\") after the end of the expression", while `a > 1` is
/// fine. The limit is narrower than it first looks and the distinction matters:
/// a parenthesised *expression* is fine, so `(a + 0) > 1` and `(data->>'k') =
/// 'x'` both work. Only wrapping a lone column in parentheses fails. The
/// expansion never touches a group without a top-level comma, so this predates
/// it; recorded here so it is not rediscovered, and stated precisely so nobody
/// rewrites working filters on the strength of a vaguer version.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shapes_that_are_not_row_comparisons_are_untouched() {
    let (_server, pool) = seeded().await;

    for (sql, expected) in [
        (
            "SELECT a,b FROM kp WHERE (a > 1) AND (b <> 'zz') ORDER BY a, b",
            vec!["2a", "2b", "3c"],
        ),
        (
            "SELECT a,b FROM kp WHERE a IN (1, 2) ORDER BY a, b",
            vec!["1x", "1y", "2a", "2b"],
        ),
        (
            "SELECT a,b FROM kp WHERE (a, b) > (1, 'x') AND a < 3 ORDER BY a, b",
            vec!["1y", "2a", "2b"],
        ),
        ("SELECT a,b FROM kp WHERE b LIKE 'a%' ORDER BY a, b", vec!["2a"]),
    ] {
        assert_eq!(pairs(&pool, sql).await, expected, "{sql}");
    }
}

/// A genuine mismatch is reported as one, rather than as a parenthesis problem.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_mismatched_row_comparison_says_so() {
    let (_server, pool) = seeded().await;
    let err = sqlx::query(AssertSqlSafe(
        "SELECT a,b FROM kp WHERE (a, b) > (1, 'x', 2)",
    ))
    .fetch_all(&pool)
    .await
    .expect_err("a row comparison with different arities was accepted");
    let text = err.to_string();
    assert!(
        text.contains("same number") || text.contains("row comparison"),
        "the error does not name the problem: {text}"
    );
}
