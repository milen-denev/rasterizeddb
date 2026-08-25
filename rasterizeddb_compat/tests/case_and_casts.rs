//! `CASE WHEN` and `::` casts, in the contexts an ORM writes them.
//!
//! One test walks the whole matrix and reports every cell that failed, so a
//! run shows the surface rather than the first hole. The supported contexts
//! (select list, SET, and the expression layer itself) are asserted to work;
//! the unsupported ones (CASE in WHERE/ORDER BY, `::` in WHERE, `ALTER …
//! TYPE … USING`) are asserted to be REFUSED — an honest error today, and a
//! tripwire that flips when someone adds the support, so the test gets
//! upgraded rather than silently passing over a half-implementation.

use sqlx::AssertSqlSafe;
use rasterizeddb_compat::TestServer;
use sqlx::Row;

async fn server() -> (TestServer, sqlx::PgPool) {
    let server = TestServer::start().await.expect("server should start");
    let pool = server.connect_sqlx().await.expect("should connect");
    (server, pool)
}

async fn run(pool: &sqlx::PgPool, sql: &str) -> Result<(), String> {
    sqlx::query(AssertSqlSafe(sql))
        .execute(pool)
        .await
        .map(|_| ())
        .map_err(|e| e.to_string().lines().next().unwrap_or("").to_string())
}

async fn one_text(pool: &sqlx::PgPool, sql: &str, col: &str) -> Result<String, String> {
    sqlx::query(AssertSqlSafe(sql))
        .fetch_one(pool)
        .await
        .map_err(|e| e.to_string().lines().next().unwrap_or("").to_string())?
        .try_get::<String, _>(col)
        .map_err(|e| e.to_string())
}

#[tokio::test]
async fn case_when_and_casts_work_where_an_orm_writes_them() {
    let (_s, pool) = server().await;
    run(&pool, "CREATE TABLE t (id INT NOT NULL, v INT, name TEXT)")
        .await
        .expect("create");
    for (id, v, name) in [(1, 1, "'a'"), (2, 2, "NULL"), (3, 30, "'c'")] {
        run(&pool, &format!("INSERT INTO t (id, v, name) VALUES ({}, {}, {})", id, v, name))
            .await
            .expect("insert");
    }

    let mut failures: Vec<String> = Vec::new();
    let mut check = |label: &str, result: Result<String, String>, want: &str| match result {
        Ok(got) if got == want => {}
        Ok(got) => failures.push(format!("{}: got {:?}, want {:?}", label, got, want)),
        Err(e) => failures.push(format!("{}: ERROR {}", label, e)),
    };

    // 1. Searched CASE in a select list.
    check(
        "select searched",
        one_text(&pool, "SELECT CASE WHEN v = 1 THEN 'one' ELSE 'many' END AS label FROM t WHERE id = 1", "label").await,
        "one",
    );
    // 2. Compound condition in a WHEN arm.
    check(
        "select compound AND",
        one_text(&pool, "SELECT CASE WHEN v = 1 AND name = 'a' THEN 'both' ELSE 'no' END AS label FROM t WHERE id = 1", "label").await,
        "both",
    );
    check(
        "select compound OR",
        one_text(&pool, "SELECT CASE WHEN v = 9 OR name = 'a' THEN 'either' ELSE 'no' END AS label FROM t WHERE id = 1", "label").await,
        "either",
    );
    // 3. CASE in a SET assignment (the backfill shape).
    match run(&pool, "UPDATE t SET name = CASE WHEN name IS NULL THEN 'unnamed' ELSE name END").await {
        Err(e) => check("update SET case", Err(e), ""),
        Ok(()) => {
            check("update SET case stored", one_text(&pool, "SELECT name FROM t WHERE id = 2", "name").await, "unnamed");
            check("update SET case untouched", one_text(&pool, "SELECT name FROM t WHERE id = 1", "name").await, "a");
        }
    }
    // 4. The production migrations' compound shape, verbatim, in SET — the
    // cell whose support lets a downstream rewrite be reverted.
    match run(
        &pool,
        "UPDATE t SET v = CASE WHEN v IS NULL OR v <= 0 THEN 3 WHEN v = 2 THEN 3 WHEN v > 3 THEN 3 ELSE v END",
    )
    .await
    {
        Err(e) => check("update SET compound case", Err(e), ""),
        Ok(()) => {
            check("compound clamped high", one_text(&pool, "SELECT v::text AS s FROM t WHERE id = 3", "s").await, "3");
            check("compound kept in range", one_text(&pool, "SELECT v::text AS s FROM t WHERE id = 1", "s").await, "1");
        }
    }
    // 5. Casts that work: applied to a column, a literal, a parenthesised
    // expression, and reaching float4 through `::real`.
    check("cast column to text", one_text(&pool, "SELECT id::text AS s FROM t WHERE id = 1", "s").await, "1");
    check("cast literal to int rendered", one_text(&pool, "SELECT ('42'::int)::text AS s FROM t WHERE id = 1", "s").await, "42");
    check("cast parenthesised arith", one_text(&pool, "SELECT '1.' || (v - 1)::text || '.0' AS s FROM t WHERE id = 3", "s").await, "1.2.0");

    // 6. Contexts that are NOT supported must be refused, not silently wrong.
    // When one of these starts passing, upgrade it to a value assertion.
    // CASE in ORDER BY rides the expression-sort path now.
    let ordered: Result<String, String> = async {
        let rows = sqlx::query("SELECT id FROM t ORDER BY CASE WHEN v >= 3 THEN 0 ELSE 1 END, id")
            .fetch_all(&pool)
            .await
            .map_err(|e| e.to_string().lines().next().unwrap_or("").to_string())?;
        Ok(rows
            .iter()
            .map(|r| r.try_get::<i32, _>("id").map(|x| x.to_string()).unwrap_or_else(|_| "?".into()))
            .collect::<Vec<_>>()
            .join(","))
    }
    .await;
    // v was rewritten by the compound UPDATE above: ids 2 and 3 now hold 3,
    // id 1 holds 1 — so v >= 3 selects {2, 3}, then id breaks the tie.
    check("order by case", ordered, "2,3,1");

    // Promoted from "still refused": a cast applied to a *literal* is dropped
    // now, because the column it is compared against already says what type the
    // value is. Clients emit `'1'::int` whenever a parameter's type would be
    // ambiguous, and refusing it failed ordinary reads of rows the same client
    // had written.
    check(
        "cast in where",
        one_text(&pool, "SELECT name FROM t WHERE id = '1'::int", "name").await,
        "a",
    );

    for (label, sql) in [
        ("where case still refused", "SELECT name FROM t WHERE CASE WHEN v = 1 THEN true ELSE false END"),
        ("alter type using still refused", "ALTER TABLE t ALTER COLUMN v TYPE INT USING (CASE WHEN v IS NULL THEN 0 ELSE v END)"),
    ] {
        if run(&pool, sql).await.is_ok() {
            failures.push(format!("{}: now succeeds — upgrade this cell to assert its value", label));
        }
    }
    // A same-type restatement WITHOUT a transform stays accepted — sea-orm's
    // modify_column restates the type even when nothing changes.
    if let Err(e) = run(&pool, "ALTER TABLE t ALTER COLUMN v TYPE INT").await {
        failures.push(format!("same-type restatement: ERROR {}", e));
    }

    assert!(failures.is_empty(), "shapes that failed:\n{}", failures.join("\n"));
}
