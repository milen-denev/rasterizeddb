//! A sweep for malformed pgwire frames.
//!
//! Production saw sqlx panic inside `bytes`: "advance out of bounds: the len
//! is 0 but advancing by 4" — a client reading a 4-byte field past the end of
//! a message body, i.e. a frame whose declared shape outruns its payload. A
//! peer session cleared plain row fetches and bound parameters over every
//! stored type, so this covers what that could not: RETURNING (plain,
//! aliased and computed), joins, aggregates, CTEs, DISTINCT, expression
//! ORDER BY and the catalogue views.
//!
//! Every probe runs through sqlx's real decoder. A malformed frame panics the
//! client task rather than returning an error, so a panic here IS the
//! finding; a plain SQL error is not — some shapes are legitimately
//! unsupported, and this test is about the wire, not the feature.

use rasterizeddb_compat::TestServer;
use sqlx::Executor;

async fn server() -> (TestServer, sqlx::PgPool) {
    let server = TestServer::start().await.expect("server should start");
    let pool = server.connect_sqlx().await.expect("should connect");
    (server, pool)
}

async fn run(pool: &sqlx::PgPool, sql: &str) -> Result<(), String> {
    pool.execute(sqlx::raw_sql(sql))
        .await
        .map(|_| ())
        .map_err(|e| e.to_string().lines().next().unwrap_or("").to_string())
}

#[tokio::test]
async fn no_shape_produces_a_malformed_frame() {
    let (_s, pool) = server().await;
    run(&pool, "CREATE TABLE a (id INT PRIMARY KEY, name TEXT, n BIGINT, doc JSONB, at TIMESTAMPTZ)")
        .await
        .expect("create a");
    run(&pool, "CREATE TABLE b (id INT PRIMARY KEY, a_id INT NOT NULL, label TEXT)")
        .await
        .expect("create b");
    for i in 1..=3 {
        run(&pool, &format!(
            "INSERT INTO a (id, name, n, doc, at) VALUES ({}, 'n{}', {}, '{{\"k\": {}}}', '2026-08-23 10:0{}:00')",
            i, i, i * 100, i, i))
            .await
            .expect("insert a");
        run(&pool, &format!("INSERT INTO b (id, a_id, label) VALUES ({}, {}, 'l{}')", i, i, i))
            .await
            .expect("insert b");
    }

    // Each probe is run through the EXTENDED protocol (fetch_all on a query
    // with a bound parameter where possible) and the simple one, since the
    // two take different description paths.
    let probes: Vec<&str> = vec![
        // Projections and aliases.
        "SELECT id, name FROM a",
        "SELECT a.id AS ident, a.name AS who FROM a",
        "SELECT * FROM a",
        "SELECT 1 AS one, 'x' AS ex FROM a",
        // Expressions in the select list.
        "SELECT id, (n - 1) AS less, LOWER(name) AS lname FROM a",
        "SELECT COALESCE(name, 'none') AS c, doc->>'k' AS k FROM a",
        // Joins.
        "SELECT a.id, b.label FROM a JOIN b ON b.a_id = a.id",
        "SELECT DISTINCT a.name FROM a JOIN b ON b.a_id = a.id",
        // Aggregates, grouped and bare.
        "SELECT COUNT(*) AS n FROM a",
        "SELECT name, COUNT(*) AS n FROM a GROUP BY name",
        "SELECT AVG(n)::bigint AS avg_n FROM a",
        "SELECT date_trunc('minute', at) AS bucket, SUM(n) AS s FROM a GROUP BY bucket",
        // Derived table and CTE.
        "SELECT COALESCE(SUM(t.s), 0) AS total FROM (SELECT SUM(n) AS s FROM a GROUP BY name) AS t",
        "WITH c AS (SELECT id FROM a WHERE n > 100) SELECT COUNT(*) AS n FROM c",
        // Expression ORDER BY with NULL placement.
        "SELECT id FROM a ORDER BY NULLIF(doc->>'k', '')::int DESC NULLS LAST, id",
        // Catalogue views.
        "SELECT table_name FROM information_schema.tables",
        // Writes with RETURNING: plain, aliased, computed, mixed, star.
        "UPDATE a SET n = n + 1 WHERE id = 1 RETURNING id",
        "UPDATE a SET n = n + 1 WHERE id = 1 RETURNING id AS ident",
        "UPDATE a SET n = n + 1 WHERE id = 1 RETURNING (n - 1) AS allocated",
        "UPDATE a SET n = n + 1 WHERE id = 1 RETURNING id, (n - 1) AS allocated, name",
        "UPDATE a SET n = n + 1 WHERE id = 1 RETURNING *",
        "INSERT INTO a (id, name, n) VALUES (90, 'ins', 5) RETURNING id",
        "INSERT INTO a (id, name, n) VALUES (91, 'ins', 5) RETURNING id, name",
        "INSERT INTO a (id, name, n) VALUES (92, 'ins', 5) RETURNING *",
        "INSERT INTO a (id, name, n) VALUES (93, 'ins', 5) RETURNING (n - 1) AS allocated",
        "DELETE FROM a WHERE id = 90 RETURNING id",
        "DELETE FROM a WHERE id = 91 RETURNING id, name",
        "DELETE FROM a b USING b v WHERE b.id = v.a_id AND b.id = 3 RETURNING b.id",
    ];

    let mut errors: Vec<String> = Vec::new();
    for sql in &probes {
        // Simple protocol.
        if let Err(e) = pool.fetch_all(sqlx::raw_sql(sql)).await {
            errors.push(format!("[simple] {} -> {}", sql, e.to_string().lines().next().unwrap_or("")));
        }
        // Extended protocol: sqlx prepares whenever `query` is used.
        if let Err(e) = sqlx::query(sql).fetch_all(&pool).await {
            errors.push(format!("[extended] {} -> {}", sql, e.to_string().lines().next().unwrap_or("")));
        }
    }

    // Errors are printed rather than asserted: an unsupported shape is a
    // feature gap, not a wire fault, and the wire fault is a panic. The
    // duplicate-key errors below are this test's own doing — each INSERT runs
    // once per protocol.
    for e in &errors {
        println!("ERR {}", e);
    }
    println!("probes: {}, errored: {}", probes.len() * 2, errors.len());
    let unexpected: Vec<&String> = errors
        .iter()
        .filter(|e| !e.contains("duplicate value for UNIQUE"))
        .collect();
    assert!(unexpected.is_empty(), "shapes that failed:\n{:#?}", unexpected);
}

/// A select item wrapped in its own parentheses is the expression inside it.
/// `SELECT (n - 1) AS less` was read as a call whose name is the empty string
/// and refused — "function '' is not supported yet" — while the identical
/// `n - 1` without parentheses worked. Every ORM emits the parenthesised
/// form, and the same shape works in `RETURNING`, so the two disagreed.
#[tokio::test]
async fn a_wholly_parenthesised_select_item_is_an_expression() {
    let (_s, pool) = server().await;
    run(&pool, "CREATE TABLE a (id INT PRIMARY KEY, name TEXT, n BIGINT)")
        .await
        .expect("create");
    run(&pool, "INSERT INTO a (id, name, n) VALUES (1, 'X', 100)")
        .await
        .expect("insert");

    use sqlx::Row;
    let row = sqlx::query("SELECT id, (n - 1) AS less, LOWER(name) AS lname FROM a")
        .fetch_one(&pool)
        .await
        .expect("a parenthesised item must be an expression");
    assert_eq!(row.try_get::<i32, _>("id").expect("id"), 1);
    assert_eq!(row.try_get::<i64, _>("less").expect("less"), 99);
    assert_eq!(row.try_get::<String, _>("lname").expect("lname"), "x");

    // Nested, and parenthesised-but-not-wholly, both still work.
    let row = sqlx::query("SELECT ((n * 2)) AS doubled, (n - 1) + 2 AS plus FROM a")
        .fetch_one(&pool)
        .await
        .expect("nested parentheses");
    assert_eq!(row.try_get::<i64, _>("doubled").expect("doubled"), 200);
    assert_eq!(row.try_get::<i64, _>("plus").expect("plus"), 101);

    // A parenthesised SELECT is a scalar subquery, not an expression — it
    // must not be captured by the peel above, which would refuse it as an
    // unparseable expression. It still resolves to its value; naming it by
    // its alias is a separate, pre-existing gap in the describe path, so the
    // value is read positionally here.
    // Through the simple protocol: over the extended one the describe path
    // cannot name this statement's columns and sqlx then discards every
    // column of the rows (rasterizeddb-29's open item — same signature as the
    // billing rollup). Flip this to the extended form once that lands.
    let rows = pool
        .fetch_all(sqlx::raw_sql("SELECT id, (SELECT COUNT(*) FROM a) AS n_rows FROM a"))
        .await
        .expect("scalar subquery still resolves");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].try_get::<i64, _>("n_rows").expect("n_rows"), 1);
}
