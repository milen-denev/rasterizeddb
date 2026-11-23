//! A comparison between two columns, either of which may be NULL.
//!
//! A NULL cell holds its type's zeroes, and only the column's NULL companion
//! says it is NULL, so every comparison over a nullable column needs a guard
//! that reads the companion. Two gaps let a NULL compare as zero:
//!
//! - Only the column a comparison started with was guarded. In `x <> y` with
//!   both nullable, a NULL `y` was `0`, and the row was kept. `NOT (x = y)`
//!   becomes `x != y` and inherited it.
//! - A join's combined row renamed each companion so that no guard could find
//!   it: `b.v`'s became `b____null_v` where `__null_b__v` was looked for. A
//!   term comparing two relations was then not guarded at all. `NOT (a.v =
//!   b.v)` used to be refused there, and took this route once `NOT` was
//!   pushed down to its comparisons.
//!
//! Every expected answer below is PostgreSQL 17's for the same rows.

use rasterizeddb_compat::TestServer;
use sqlx::AssertSqlSafe;
use sqlx::{Executor, Row};

const SETUP: &[&str] = &[
    "CREATE TABLE a (id INT PRIMARY KEY, v INT, name TEXT, w INT)",
    "CREATE TABLE b (id INT PRIMARY KEY, a_id INT NOT NULL, v INT, name TEXT, w INT)",
    "INSERT INTO a VALUES (1,5,'p',0),(2,0,'q',NULL),(3,7,NULL,3),(4,NULL,'r',1),(5,0,'',0)",
    "INSERT INTO b VALUES (1,1,NULL,NULL,0),(2,2,NULL,'q',7),(3,3,7,'x',NULL),(4,4,0,NULL,NULL),\
     (5,1,5,'p',0),(6,5,NULL,NULL,NULL),(7,2,0,'',NULL),(8,1,9,'z',4),(9,3,1,'y',3)",
    "CREATE TABLE c (id INT PRIMARY KEY, x INT, y INT, z INT NOT NULL)",
    "INSERT INTO c VALUES (1,5,NULL,5),(2,NULL,0,0),(3,NULL,NULL,0),(4,0,0,1),(5,3,4,4),(6,0,NULL,0),(7,4,3,9)",
];

/// `(predicate, the a-b pairs PostgreSQL 17 returned)` for
/// `SELECT string_agg(a.id || '-' || b.id, ',' ORDER BY a.id, b.id)
///  FROM a JOIN b ON a.id = b.a_id WHERE <predicate>`.
const JOIN_CASES: &[(&str, &str)] = &[
    ("NOT (a.v = b.v)", "1-8,3-9"),
    ("a.v <> b.v", "1-8,3-9"),
    ("a.v != b.v", "1-8,3-9"),
    ("a.v = b.v", "1-5,2-7,3-3"),
    ("NOT (a.v <> b.v)", "1-5,2-7,3-3"),
    ("a.v < b.v", "1-8"),
    ("a.v >= b.v", "1-5,2-7,3-3,3-9"),
    ("NOT (a.v < b.v)", "1-5,2-7,3-3,3-9"),
    ("NOT a.v = b.v", "1-8,3-9"),
    ("a.name = b.name", "1-5,2-2"),
    ("a.name <> b.name", "1-8,2-7"),
    ("NOT (a.name = b.name)", "1-8,2-7"),
    ("a.name < b.name", "1-8"),
    ("a.w = b.w", "1-1,1-5,3-9"),
    ("a.w <> b.w", "1-8"),
    ("NOT (a.w = b.w)", "1-8"),
    ("a.w <= b.w", "1-1,1-5,1-8,3-9"),
    ("NOT (a.v = b.v OR a.name = b.name)", "1-8"),
    ("NOT (a.v = b.v AND a.w = b.w)", "1-8,3-9"),
    ("NOT (a.v = b.v) AND a.id > 1", "3-9"),
    ("a.v + 1 > b.v", "1-5,2-7,3-3,3-9"),
    ("NOT (a.v + 1 > b.v)", "1-8"),
    ("a.v = b.v OR a.w = b.w", "1-1,1-5,2-7,3-3,3-9"),
    ("a.v IN (b.v, b.w)", "1-5,2-7,3-3"),
    ("a.v NOT IN (b.v, b.w)", "1-8,3-9"),
    ("NOT (a.v IN (b.v, b.w))", "1-8,3-9"),
    ("NOT (a.v BETWEEN b.v AND b.w)", "1-1,1-5,1-8,3-9"),
    ("a.v IS DISTINCT FROM b.v", "1-1,1-8,2-2,3-9,4-4,5-6"),
    ("NOT (a.v IS DISTINCT FROM b.v)", "1-5,2-7,3-3"),
    ("a.v IS NOT DISTINCT FROM b.v", "1-5,2-7,3-3"),
    ("NOT NOT (a.v = b.v)", "1-5,2-7,3-3"),
];

/// Forms the engine does not support over a join, which it may refuse but
/// must never answer wrongly. A column as a LIKE pattern used to be read as
/// the text of its name and matched nothing.
const JOIN_REFUSED_OR_RIGHT: &[(&str, &str)] = &[
    ("a.name LIKE b.name", "1-5,2-2"),
    ("NOT (a.name LIKE b.name)", "1-8,2-7"),
    ("a.name NOT LIKE b.name", "1-8,2-7"),
    ("a.v BETWEEN b.v AND b.w", ""),
    ("a.v = b.v IS NOT TRUE", "1-1,1-8,2-2,3-9,4-4,5-6"),
];

/// `(predicate, the ids PostgreSQL 17 returned)` for
/// `SELECT string_agg(id::text, ',' ORDER BY id) FROM c WHERE <predicate>`,
/// where `x` and `y` are nullable and `z` is not.
const TABLE_CASES: &[(&str, &str)] = &[
    ("x = y", "4"),
    ("x <> y", "5,7"),
    ("NOT (x = y)", "5,7"),
    ("x < y", "5"),
    ("NOT (x >= y)", "5"),
    ("z = y", "2,5"),
    ("z <> y", "4,7"),
    ("NOT (z = y)", "4,7"),
    ("z + 1 > y", "2,4,5,7"),
    ("NOT (z + 1 > y)", ""),
    ("x + 1 > y", "4,7"),
    ("x IN (y, z)", "1,4,6"),
    ("x NOT IN (y, z)", "5,7"),
    ("NOT (x IN (y, 1))", "5,7"),
    ("z BETWEEN x AND y", "5"),
    ("NOT (z BETWEEN x AND y)", "4,7"),
    ("z NOT BETWEEN x AND y", "4,7"),
    ("x IS DISTINCT FROM y", "1,2,5,6,7"),
    ("x IS NOT DISTINCT FROM y", "3,4"),
    ("z IS DISTINCT FROM y", "1,3,4,6,7"),
    ("NOT (z IS DISTINCT FROM y)", "2,5"),
    ("x = y OR id = 1", "1,4"),
    ("NOT (x = y) OR id = 2", "2,5,7"),
    ("NOT (x = y AND z = 0)", "1,4,5,7"),
];

async fn pairs(pool: &sqlx::PgPool, predicate: &str) -> Result<String, String> {
    let sql = format!(
        "SELECT a.id AS l, b.id AS r FROM a JOIN b ON a.id = b.a_id WHERE {predicate} ORDER BY a.id, b.id"
    );
    match sqlx::query(AssertSqlSafe(sql)).fetch_all(pool).await {
        Ok(rows) => Ok(rows
            .iter()
            .map(|row| format!("{}-{}", row.get::<i32, _>("l"), row.get::<i32, _>("r")))
            .collect::<Vec<_>>()
            .join(",")),
        Err(e) => Err(e.to_string()),
    }
}

async fn ids(pool: &sqlx::PgPool, predicate: &str) -> Result<String, String> {
    let sql = format!("SELECT id FROM c WHERE {predicate} ORDER BY id");
    match sqlx::query(AssertSqlSafe(sql)).fetch_all(pool).await {
        Ok(rows) => Ok(rows
            .iter()
            .map(|row| row.get::<i32, _>("id").to_string())
            .collect::<Vec<_>>()
            .join(",")),
        Err(e) => Err(e.to_string()),
    }
}

async fn server() -> (TestServer, sqlx::PgPool) {
    let server = TestServer::start().await.expect("server should start");
    let pool = server.connect_sqlx().await.expect("should connect");
    for statement in SETUP {
        pool.execute(sqlx::raw_sql(*statement)).await.expect(statement);
    }
    (server, pool)
}

#[tokio::test(flavor = "multi_thread")]
async fn cross_relation_predicates_over_null_answer_as_postgresql_does() {
    let (_s, pool) = server().await;
    let mut wrong = Vec::new();
    for (predicate, want) in JOIN_CASES {
        let got = pairs(&pool, predicate).await;
        if got.as_deref() != Ok(*want) {
            wrong.push(format!("{predicate:<40} want {want:<30} got {got:?}"));
        }
    }
    for (predicate, want) in JOIN_REFUSED_OR_RIGHT {
        match pairs(&pool, predicate).await {
            Err(_) => {}
            Ok(got) if got == *want => {}
            got => wrong.push(format!("{predicate:<40} want {want:<30} or an error, got {got:?}")),
        }
    }
    let total = JOIN_CASES.len() + JOIN_REFUSED_OR_RIGHT.len();
    assert!(wrong.is_empty(), "{} of {} wrong:\n{}", wrong.len(), total, wrong.join("\n"));
}

#[tokio::test(flavor = "multi_thread")]
async fn comparisons_between_nullable_columns_answer_as_postgresql_does() {
    let (_s, pool) = server().await;
    let mut wrong = Vec::new();
    for (predicate, want) in TABLE_CASES {
        let got = ids(&pool, predicate).await;
        if got.as_deref() != Ok(*want) {
            wrong.push(format!("{predicate:<30} want {want:<20} got {got:?}"));
        }
    }
    assert!(wrong.is_empty(), "{} of {} wrong:\n{}", wrong.len(), TABLE_CASES.len(), wrong.join("\n"));
}
