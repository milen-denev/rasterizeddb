//! `LIKE` on a JSON path reads its pattern exactly as on a text column: a
//! pattern ending in its escape character is refused with 22025, `ESCAPE`
//! picks the escape character (`ESCAPE ''` turns escaping off), and a path
//! that reaches nothing is NULL, which neither `LIKE` nor `NOT LIKE` matches.
//! The path form used to hand the raw pattern to the matcher, which read a
//! lone trailing backslash as a literal and knew no `ESCAPE` at all.

use rasterizeddb_compat::TestServer;
use sqlx::AssertSqlSafe;
use sqlx::{Executor, Row};

#[tokio::test(flavor = "multi_thread")]
async fn like_on_a_path_reads_its_pattern_as_a_column_does() {
    let server = TestServer::start().await.expect("server");
    let pool = server.connect_sqlx().await.expect("connect");
    pool.execute(sqlx::raw_sql("CREATE TABLE l (id BIGINT PRIMARY KEY, doc JSONB, s TEXT)"))
        .await
        .expect("create");
    pool.execute(sqlx::raw_sql(
        r#"INSERT INTO l VALUES (1,'{"c":"a%b"}','a%b'),(2,'{"c":"axb"}','axb'),(3,'{}',NULL),(4,'{"c":"a\\b"}','a\b')"#,
    ))
    .await
    .expect("seed");

    let ids = |clause: String| {
        let pool = pool.clone();
        async move {
            let sql = format!("SELECT id FROM l WHERE {clause} ORDER BY id");
            sqlx::query(AssertSqlSafe(sql.clone()))
                .fetch_all(&pool)
                .await
                .map(|rows| rows.iter().map(|r| r.get::<i64, _>("id").to_string()).collect::<Vec<_>>().join(","))
                .map_err(|e| e.as_database_error().and_then(|d| d.code().map(|c| c.to_string())).unwrap_or_else(|| e.to_string()))
        }
    };

    let mut wrong = Vec::new();
    // Each pattern is asked of the path and of the text column holding the
    // same strings, which must agree. The answers are PostgreSQL 17's.
    for (pattern, want) in [
        ("LIKE 'a%b'", Ok("1,2,4")),
        ("LIKE 'a#%b' ESCAPE '#'", Ok("1")),
        ("NOT LIKE 'a#%b' ESCAPE '#'", Ok("2,4")),
        (r"LIKE 'a\%b'", Ok("1")),
        (r"LIKE 'a\\b'", Ok("4")),
        // No escape: `\` is a literal and `%` still a wildcard, so `a\b` matches.
        (r"LIKE 'a\%b' ESCAPE ''", Ok("4")),
        (r"LIKE 'a\b' ESCAPE ''", Ok("4")),
        ("ILIKE 'A%B'", Ok("1,2,4")),
        // Every string here is three characters with `a` and `b` at the ends.
        ("NOT ILIKE 'A_B'", Ok("")),
        (r"LIKE 'a\'", Err("22025")),
        (r"NOT LIKE 'a\'", Err("22025")),
        ("LIKE 'a#' ESCAPE '#'", Err("22025")),
    ] {
        for operand in ["doc->>'c'", "s"] {
            let got = ids(format!("{operand} {pattern}")).await;
            let want = want.map(str::to_string).map_err(str::to_string);
            if got != want {
                wrong.push(format!("{operand} {pattern}: got {got:?}, want {want:?}"));
            }
        }
    }
    assert!(wrong.is_empty(), "{} wrong:\n{}", wrong.len(), wrong.join("\n"));
}
